package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzePair;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzeResult;
import com.viskan.payloadbuilder.parser.AJoin;
import com.viskan.payloadbuilder.parser.ASelectVisitor;
import com.viskan.payloadbuilder.parser.Apply;
import com.viskan.payloadbuilder.parser.Apply.ApplyType;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.ExpressionSelectItem;
import com.viskan.payloadbuilder.parser.Join;
import com.viskan.payloadbuilder.parser.Join.JoinType;
import com.viskan.payloadbuilder.parser.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.NestedSelectItem;
import com.viskan.payloadbuilder.parser.NestedSelectItem.Type;
import com.viskan.payloadbuilder.parser.PopulateTableSource;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.Select;
import com.viskan.payloadbuilder.parser.SelectItem;
import com.viskan.payloadbuilder.parser.SelectStatement;
import com.viskan.payloadbuilder.parser.SortItem;
import com.viskan.payloadbuilder.parser.Table;
import com.viskan.payloadbuilder.parser.TableFunction;
import com.viskan.payloadbuilder.parser.TableOption.BatchSizeOption;
import com.viskan.payloadbuilder.parser.TableSource;
import com.viskan.payloadbuilder.parser.TableSourceJoined;

import static com.viskan.payloadbuilder.operator.OperatorBuilderUtils.createGroupBy;
import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;

import gnu.trove.map.hash.THashMap;

/**
 * Builder that constructs a {@link Operator} and {@link Projection} for a {@link SelectStatement}
 */
public class OperatorBuilder extends ASelectVisitor<Void, OperatorBuilder.Context>
{
    private static final OperatorBuilder VISITOR = new OperatorBuilder();

    private static final BiFunction<String, TableAlias, RuntimeException> MULTIPLE_ALIAS_EXCEPTION = (alias,
            parent) -> new IllegalArgumentException("Alias " + alias + " is defined multiple times for parent: " + parent);

    /** Context used during visiting tree */
    static class Context
    {
        /** Counter for unique node ids */
        private int nodeId;

        private QuerySession session;
        private TableAlias parent;
        Map<TableAlias, Set<String>> columnsByAlias = new THashMap<>();

        /** Resulting operator */
        private Operator operator;

        /** Resulting projection */
        private Projection projection;

        /**
         * Alias that should override provided alias to {@link #appendTableAlias(QualifiedName, String)} Used in populating joins
         */
        private String alias;

        /** Should new parent be set upon calling {@link #appendTableAlias(QualifiedName, String)} */
        private boolean setParent;

        /** Index if any that should be picked up when creating table operator */
        private Index index;
        
        /** Predicate pushed down from join to table source */
        private final Map<String, Expression> pushDownPredicateByAlias = new THashMap<>();



        /** Acquire next unique node id */
        private int acquireNodeId()
        {
            int result = nodeId;
            nodeId++;
            return result;
        }

        /** Appends push down predicate for provided alias */
        private void appendPushDownPredicate(String alias, Expression expression)
        {
            if (expression == null)
            {
                return;
            }
            pushDownPredicateByAlias.compute(alias, (k, v) ->
            {
                if (v == null)
                {
                    return expression;
                }

                return new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, v, expression);
            });
        }

        /** Appends table alias to hierarchy */
        private TableAlias appendTableAlias(QualifiedName table, String alias, String[] columns)
        {
            String aliasToUse = this.alias != null ? this.alias : alias;

            if (parent == null)
            {
                parent = new TableAlias(null, table, aliasToUse, columns);
                return parent;
            }
            else
            {
                if (parent.getChildAlias(aliasToUse) != null)
                {
                    throw MULTIPLE_ALIAS_EXCEPTION.apply(aliasToUse, parent);
                }

                TableAlias current = parent;
                while (current != null)
                {
                    if (alias.equals(current.getAlias()))
                    {
                        throw MULTIPLE_ALIAS_EXCEPTION.apply(aliasToUse, parent);
                    }
                    current = current.getParent();
                }

                TableAlias newAlias = new TableAlias(parent, table, aliasToUse, columns);
                if (setParent)
                {
                    parent = newAlias;
                }

                return newAlias;
            }
        }
    }

    /** Create operator */
    public static Pair<Operator, Projection> create(QuerySession session, Select select)
    {
        Context context = new Context();
        context.session = session;
        select.accept(VISITOR, context);
        context.columnsByAlias.entrySet().forEach(e ->
        {
            if (e.getKey() != null && e.getKey().getColumns() == null)
            {
                e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY));
            }
        });
        return Pair.of(context.operator, context.projection);
    }

    @Override
    public Void visit(Select query, Context context)
    {
        TableSourceJoined tsj = query.getFrom();
        List<AJoin> joins = tsj != null? tsj.getJoins() : emptyList();
        int joinSize = joins.size();
        
        Expression where = query.getWhere();
        Expression pushDownPredicate = null;
        if (where != null && joinSize > 0)
        {
            /* TODO:
              Push down can applied to non populating joins ie:
              select s.id
              from source s
              inner join article a
                on a.id = s.id
              inner join brand b
                on b.id = a.bid
              where s.id > 0        <--- Pushed down to source (DONE!)
              and a.id2 < 10        <--- Can be pushed to article
              and b.id3 != 10       <--- can be pushed to brand

            */
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where);
            Pair<Expression, AnalyzeResult> pair = analyzeResult.extractPushdownPredicate(tsj.getTableSource().getAlias(), true);
            if (pair.getKey() != null)
            {
                pushDownPredicate = pair.getKey();
                where = pair.getValue().getPredicate();
            }
        }

        
        int batchSize = -1;
        int batchLimitId = -1;
        // No need to batch when there is no joins
        if (joinSize > 0)
        {
            batchSize = getBatchSize(tsj.getTableSource());
        }
        // TODO: Utilize index of "from" clause if applicable
        //       ie. s.id = 10, s.id in (1,2,3,4,5)
        if (tsj != null)
        {
            tsj.getTableSource().accept(this, context);
        }
        
        // Collect columns from pushed down predicate
        if (pushDownPredicate != null)
        {
            context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(pushDownPredicate));
            visit(pushDownPredicate, context);
        }

        if (batchSize > 0)
        {
            batchLimitId = context.acquireNodeId();
            context.operator = new BatchLimitOperator(batchLimitId, context.operator, batchSize);
        }

        for (int i = 0; i < joinSize; i++)
        {
            joins.get(i).accept(this, context);
        }

        if (where != null)
        {
            context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(where));
            visit(where, context);
        }

        List<Expression> groupBys = query.getGroupBy();
        if (!query.getGroupBy().isEmpty())
        {
            groupBys.forEach(e -> visit(e, context));
            context.operator = createGroupBy(context.acquireNodeId(), tsj.getTableSource().getAlias(), groupBys, context.operator);
        }

        if (batchSize > 0)
        {
            context.operator = new BatchRepeatOperator(context.acquireNodeId(), batchLimitId, context.operator);
        }

        // TODO: wrap context.operator with order by
        // NOTE! Must be after batch repeat operator or else the order will be per batch
        List<SortItem> sortBys = query.getOrderBy();
        if (!sortBys.isEmpty())
        {
            sortBys.forEach(si -> si.accept(this, context));
        }

        List<String> projectionAliases = new ArrayList<>();
        List<Projection> projections = new ArrayList<>();
        query.getSelectItems().forEach(s ->
        {
            s.accept(this, context);
            projectionAliases.add(s.getIdentifier());
            projections.add(context.projection);
        });

        context.projection = new ObjectProjection(projectionAliases, projections);
        return null;
    }

    @Override
    public Void visit(NestedSelectItem nestedSelectItem, Context context)
    {
        Set<TableAlias> aliases = asSet(context.parent);
        Operator fromOperator = null;
        Expression from = nestedSelectItem.getFrom();
        if (from != null)
        {
            fromOperator = new ExpressionOperator(context.acquireNodeId(), from);
            aliases = ColumnsVisitor.getColumnsByAlias(context.session, context.columnsByAlias, context.parent, from);
        }

        List<String> projectionAliases = new ArrayList<>();
        List<Projection> projections = new ArrayList<>();
        boolean projectionsAdded = false;

        // Use found aliases and traverse select items, order, groupBy and where
        TableAlias parent = context.parent;
        for (TableAlias alias : aliases)
        {
            context.parent = alias;
            for (SelectItem s : nestedSelectItem.getSelectItems())
            {
                s.accept(this, context);
                if (!projectionsAdded)
                {
                    projectionAliases.add(s.getIdentifier());
                    projections.add(context.projection);
                }
            }

            if (nestedSelectItem.getWhere() != null)
            {
                visit(nestedSelectItem.getWhere(), context);
            }

            if (!nestedSelectItem.getGroupBy().isEmpty())
            {
                nestedSelectItem.getGroupBy().forEach(gb -> visit(gb, context));
            }

            if (!nestedSelectItem.getOrderBy().isEmpty())
            {
                nestedSelectItem.getOrderBy().forEach(si -> si.accept(this, context));
            }

            projectionsAdded = true;
        }

        if (nestedSelectItem.getWhere() != null)
        {
            fromOperator = new FilterOperator(context.acquireNodeId(), fromOperator, new ExpressionPredicate(nestedSelectItem.getWhere()));
        }

        if (!nestedSelectItem.getGroupBy().isEmpty())
        {
            fromOperator = createGroupBy(context.acquireNodeId(), parent.getAlias(), nestedSelectItem.getGroupBy(), fromOperator);
        }

        if (!nestedSelectItem.getOrderBy().isEmpty())
        {
            // TODO: wrap from operator with order by
        }

        if (nestedSelectItem.getType() == Type.ARRAY)
        {
            context.projection = new ArrayProjection(projections, fromOperator);
        }
        else
        {
            context.projection = new ObjectProjection(projectionAliases, projections, fromOperator);
        }
        context.parent = parent;
        return null;
    }

    @Override
    public Void visit(ExpressionSelectItem selectItem, Context context)
    {
        Expression expression = selectItem.getExpression();
        visit(expression, context);
        context.projection = new ExpressionProjection(expression);
        return null;
    }

    @Override
    protected void visit(Expression expression, Context context)
    {
        // Resolve columns
        ColumnsVisitor.getColumnsByAlias(context.session, context.columnsByAlias, context.parent, expression);
    }

    @Override
    public Void visit(Join join, Context context)
    {
        join(context,
                (join.getType() == JoinType.LEFT ? "LEFT" : "INNER") + " JOIN",
                join,
                join.getCondition(),
                join.getType() == JoinType.LEFT);
        return null;
    }

    @Override
    public Void visit(Apply apply, Context context)
    {
        join(context,
                (apply.getType() == ApplyType.OUTER ? "OUTER" : "CROSS") + " APPLY",
                apply,
                null,
                apply.getType() == ApplyType.OUTER);
        return null;
    }

    @Override
    public Void visit(PopulateTableSource tableSource, Context context)
    {
        // Store current parent alias
        TableAlias parent = context.parent;
        TableSourceJoined tsj = tableSource.getTableSourceJoined();

        Expression where = tableSource.getWhere();
        Expression pushDownPredicate = null;
        if (where != null)
        {
            // Analyze expression to see if there is push down candidates
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where);
            Pair<Expression, AnalyzeResult> pair = analyzeResult.extractPushdownPredicate(tableSource.getAlias(), true);
            if (pair.getKey() != null)
            {
                pushDownPredicate = pair.getKey();
                where = pair.getValue().getPredicate();
                context.appendPushDownPredicate(tableSource.getAlias(), pushDownPredicate);
            }
        }

        context.setParent = true;
        // Force populate table source alias
        context.alias = tableSource.getAlias();
        tsj.getTableSource().accept(this, context);
        context.alias = null;
        context.setParent = false;

        // Analyze pushed down predicate for columns
        // Need to be performed post visit of tableSource above
        // to have correct table alias in context
        if (pushDownPredicate != null)
        {
            visit(pushDownPredicate, context);
        }
        pushDownPredicate = context.pushDownPredicateByAlias.remove(tableSource.getAlias());
        if (pushDownPredicate != null)
        {
            context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(pushDownPredicate));
        }

        // Child joins
        tsj.getJoins().forEach(j -> j.accept(this, context));

        // Left over where after push down split, apply a filter
        if (where != null)
        {
            visit(where, context);
            context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(where));
        }
        List<Expression> groupBys = tableSource.getGroupBy();
        if (!groupBys.isEmpty())
        {
            groupBys.forEach(e -> visit(e, context));
            context.operator = createGroupBy(context.acquireNodeId(), tsj.getTableSource().getAlias(), groupBys, context.operator);
        }
        // TODO: wrap context.operator with order by
        List<SortItem> sortBys = tableSource.getOrderBy();
        if (!sortBys.isEmpty())
        {
            sortBys.forEach(si -> si.accept(this, context));
        }
        
        // Restore values before leaving
        context.parent = parent;
        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        Pair<Catalog, QualifiedName> pair = getCatalogAndTable(context, table.getTable());
        TableAlias alias = context.appendTableAlias(pair.getValue(), table.getAlias(), null);
        int nodeId = context.acquireNodeId();
        if (context.index != null)
        {
            context.operator = pair.getKey().getIndexOperator(nodeId, alias, context.index);
            context.index = null;
        }
        else
        {
            context.operator = pair.getKey().getScanOperator(nodeId, alias);
        }
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo(context.session);
        TableAlias alias = context.appendTableAlias(
                QualifiedName.of(functionInfo.getCatalog().getName(), functionInfo.getName()),
                tableFunction.getAlias(),
                functionInfo.getColumns());
        tableFunction.getArguments().forEach(a -> visit(a, context));
        context.operator = new TableFunctionOperator(context.acquireNodeId(), alias, functionInfo, tableFunction.getArguments());
        return null;
    }

    //    private boolean detectCorrelated(TableSource tableSource, Context context)
    //    {
    //        // Store aliases before visiting table source to be able to detect if this join is correlated
    //        Map<TableAlias, Set<String>> columnsByAlias = context.columnsByAlias;
    //        context.columnsByAlias = new THashMap<>();
    //
    //        // If there are any references to current parent
    //        // or any parent or parents children, then table source is correlated
    //        Set<TableAlias> parents = new THashSet<>();
    //        parents.add(context.parent);
    //        TableAlias current = context.parent.getParent();
    //        while (current != null)
    //        {
    //            parents.add(current);
    //            parents.addAll(current.getChildAliases());
    //            current = current.getParent();
    //        }
    //
    //        tableSource.accept(this, context);
    //
    //        boolean isCorrelated = context.columnsByAlias.keySet().stream().anyMatch(alias -> parents.contains(alias));
    //
    //        columnsByAlias.putAll(context.columnsByAlias);
    //        context.columnsByAlias = columnsByAlias;
    //
    //        return isCorrelated;
    //    }

    /** Returns a batch size if any on provided table source */
    private int getBatchSize(TableSource ts)
    {
        if (ts instanceof Table)
        {
            Table table = (Table) ts;
            return table
                    .getOptions()
                    .stream()
                    .filter(o -> o instanceof BatchSizeOption)
                    .mapToInt(o -> ((BatchSizeOption) o).getBatchSize())
                    .findFirst()
                    .orElse(-1);
        }

        return -1;
    }

    private void join(
            Context context,
            String logicalOperator,
            AJoin join,
            Expression condition,
            boolean emitEmptyOuterRows)
    {
        Operator outer = context.operator;
        context.operator = null;

        Operator joinOperator = createJoin(
                context,
                logicalOperator,
                outer,
                join.getTableSource(),
                condition,
                emitEmptyOuterRows,
                false);

        if (condition != null)
        {
            visit(condition, context);
        }
        context.operator = joinOperator;
    }

    private Operator createJoin(
            Context context,
            String logicalOperator,
            Operator outer,
            TableSource innerTableSource,
            final Expression joinCondition,
            boolean emitEmptyOuterRows,
            boolean isCorrelated)
    {
        Expression condition = joinCondition;
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(joinCondition);
        String innerAlias = innerTableSource.getAlias();
        boolean populating = innerTableSource instanceof PopulateTableSource;
        List<AnalyzePair> equiPairs = analyzeResult.getEquiPairs(innerAlias, true);
        int equiPairSize = equiPairs.size();

        // TODO: correlated

        /* No equi items in condition => NestedLoop */
        if (equiPairSize == 0)
        {
            condition = extractPushDownPredicate(context, innerAlias, emitEmptyOuterRows, analyzeResult);
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

            /* Cache inner operator */
            if (!isCorrelated)
            {
                inner = new CachingOperator(context.acquireNodeId(), inner);
            }

            return new NestedLoopJoin(
                    context.acquireNodeId(),
                    logicalOperator,
                    outer,
                    inner,
                    condition != null ? new ExpressionPredicate(condition) : null,
                    DefaultRowMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        List<Index> indices = emptyList();

        if (innerTableSource.getTable() != null)
        {
            Pair<Catalog, QualifiedName> pair = getCatalogAndTable(context, innerTableSource.getTable());
            Catalog catalog = pair.getKey();
            QualifiedName table = pair.getValue();
            indices = catalog.getIndices(table);
        }
        // TODO: If outer is ordered and no inner indices
        // then the inner could be created and after check it's order for a potential
        // MergeJoin

        /* Extract all references inner columns from equi items */
        Set<String> columnItems = equiPairs
                .stream()
                .map(pair -> pair.getColumn(innerAlias, true))
                .filter(Objects::nonNull)
                .collect(toSet());

        Index index = null;

        if (!columnItems.isEmpty())
        {
            /* Find the first matching index from extracted columns above */
            index = indices
                    .stream()
                    .filter(i -> columnItems.containsAll(i.getColumns()))
                    .findFirst()
                    .orElse(null);
        }

        List<Expression> outerValueExpressions = new ArrayList<>(equiPairSize);
        List<Expression> innerValueExpressions = new ArrayList<>(equiPairSize);
        List<AnalyzePair> indexPairs = new ArrayList<>(equiPairSize);
        populateValueExtractors(
                innerAlias,
                equiPairs,
                index,
                outerValueExpressions,
                innerValueExpressions,
                indexPairs);

        // No indices for inner -> HashJoin
        if (index == null)
        {
            condition = extractPushDownPredicate(context, innerAlias, emitEmptyOuterRows, analyzeResult);
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

            return new HashJoin(
                    context.acquireNodeId(),                    
                    logicalOperator,
                    outer,
                    inner,
                    // Collect outer hash expressions
                    new ExpressionHashFunction(outerValueExpressions),
                    // Collect inner hash expression
                    new ExpressionHashFunction(innerValueExpressions),
                    new ExpressionPredicate(condition),
                    DefaultRowMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        List<AnalyzePair> pairs = new ArrayList<>(analyzeResult.pairs);
        pairs.removeAll(indexPairs);

        // Remove all pushdown candidates from index items,
        // these are contained within index and no need to push down
        // and is neither needed in join condition
        indexPairs.removeIf(pair -> pair.isPushDown(innerAlias, true));

        // Create a new analyze result from resulting pairs
        pairs.addAll(indexPairs);
        analyzeResult = new AnalyzeResult(pairs);

        // Extract pushdown and new join condition
        condition = extractPushDownPredicate(context, innerAlias, emitEmptyOuterRows, analyzeResult);
        context.index = index;
        innerTableSource.accept(this, context);
        if (context.index != null)
        {
            throw new RuntimeException("Index " + index + " should have been consumed by " + innerTableSource.getTable());
        }
        Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

        constantalizeValueExtractors(outerValueExpressions, innerValueExpressions);
        ExpressionValuesExtractor outerValuesExtractor = new ExpressionValuesExtractor(outerValueExpressions);
        ExpressionValuesExtractor innerValuesExtractor = new ExpressionValuesExtractor(innerValueExpressions);

        return new BatchHashJoin(
                context.acquireNodeId(),
                logicalOperator,
                outer,
                inner,
                outerValuesExtractor,
                innerValuesExtractor,
                new ExpressionPredicate(condition),
                DefaultRowMerger.DEFAULT,
                populating,
                emitEmptyOuterRows,
                index);

        // If outer is sorted on index keys (fully or partly)
        //
        // Or of outer be sorted on index keys
        //   => BatchMergeJoin

        // BatchHashJoin

        // We have an index
        // BatchMergeJoin
        //
        // BatchHashMatch
        //

        //        1. visit source
        //        create operator
        //          2. visit join
        //        - analyze condition
        //        - check order of outer
        //            - can we sort?
        //        - check index of inner
        //            -
        //        - decide batch reader (hint in context)
        //        - decide join type
        //        - visit article
        //            create operator check context for type
        //        - check order of inner
        //        - create join with outer and inner

        //        throw new RuntimeException("No operator could be created for join");
    }

    /**
     * <pre>
     *
     * Populates value extractors for provided index.
     *
     * Example
     *
     *   Index [club_id, country_id, art_id]
     *   Condition
     *   a.art_id = s.art_id
     *   a.country_id = 0
     *   a.club_id = 10
     *
     * Result:
     *  outerValueExpressions: [10,        0,            s.art_id]
     *  innerValueExpressions: [a.club_id, a.country_id, a.art_id]
     *  indexPairs:
     *    [a.art_id,      s.art_id],
     *    [a.country_id,  0],
     *    [a.club_id,     10]
     * </pre>
     **/
    private void populateValueExtractors(
            String innerAlias,
            List<AnalyzePair> equiPairs,
            Index index,
            List<Expression> outerValueExpressions,
            List<Expression> innerValueExpressions,
            List<AnalyzePair> indexPairs)
    {
        int size = equiPairs.size();
        for (int i = 0; i < size; i++)
        {
            AnalyzePair pair = equiPairs.get(i);

            // Find out the index of the
            String column = pair.getColumn(innerAlias, true);
            int columnIndex = 0;
            if (index != null && column != null)
            {
                columnIndex = index.getColumns().indexOf(column);
                if (columnIndex == -1)
                {
                    continue;
                }
                indexPairs.add(pair);
            }

            Pair<Expression, Expression> p = pair.getExpressionPair(innerAlias, true);

            if (outerValueExpressions.size() > columnIndex)
            {
                outerValueExpressions.add(columnIndex, p.getRight());
                innerValueExpressions.add(columnIndex, p.getLeft());
            }
            else
            {
                outerValueExpressions.add(p.getRight());
                innerValueExpressions.add(p.getLeft());
            }
        }
    }

    /**
     * <pre>
     *
     * Extracts pushdown predicate from provided analyze result
     * and pushes it to context for later extract when operator is created
     * </pre>
     *
     * @return Returns the resulting join condition
     */
    private Expression extractPushDownPredicate(
            Context context,
            String innerAlias,
            boolean emitEmptyOuterRows,
            AnalyzeResult analyzeResult)
    {
        if (!emitEmptyOuterRows)
        {
            Pair<Expression, AnalyzeResult> pushdownPair = analyzeResult.extractPushdownPredicate(innerAlias, true);
            if (pushdownPair.getKey() != null)
            {
                context.appendPushDownPredicate(innerAlias, pushdownPair.getKey());
                return pushdownPair.getValue().getPredicate();
            }
        }

        return analyzeResult.getPredicate();
    }

    /**
     * <pre>
     * Replicates constants from left/right value extractors
     * Ie. If we have an index [art_id, country_id]
     * With the following expression extractors
     *
     * Outer expressions [s.art_id, 0]
     * Inner expression  [a.art_id, a.country_id]
     *
     * Then we can simply replace a.country_id with 0
     * since values will be fetched from index and we know
     * that inner rows coming back are all having a.country_id = 0
     * and there is no need to evaluate a.country for each inner row
     * </pre>
     **/
    private void constantalizeValueExtractors(List<Expression> outerValueExpressions, List<Expression> innerValueExpressions)
    {
        int size = outerValueExpressions.size();
        for (int i = 0; i < size; i++)
        {
            if (outerValueExpressions.get(i).isConstant())
            {
                innerValueExpressions.set(i, outerValueExpressions.get(i));
            }
            else if (innerValueExpressions.get(i).isConstant())
            {
                outerValueExpressions.set(i, innerValueExpressions.get(i));
            }
        }
    }

    /** Wraps provided operation with push down predicate filter for provided alias **/
    private Operator wrapWithPushDown(Context context, Operator operator, String alias)
    {
        Expression pushDownPredicate = context.pushDownPredicateByAlias.remove(alias);
        /* Apply any pushdown filter */
        if (pushDownPredicate != null)
        {
            return new FilterOperator(context.acquireNodeId(), operator, new ExpressionPredicate(pushDownPredicate));
        }
        return operator;
    }

    /** Fetch catalog and resulting qualifed name from provided table */
    private Pair<Catalog, QualifiedName> getCatalogAndTable(Context context, final QualifiedName table)
    {
        requireNonNull(table, "table");
        QualifiedName tableName = table;
        Catalog catalog = context.session.getCatalogRegistry().getCatalog(tableName.getFirst());
        if (catalog == null)
        {
            catalog = context.session.getDefaultCatalog();
            if (catalog == null)
            {
                throw new IllegalArgumentException("No default catalog set");
            }
        }
        else
        {
            // Remove catalog name from table name
            tableName = tableName.extract(1, tableName.getParts().size());
        }

        return Pair.of(catalog, tableName);
    }
}
