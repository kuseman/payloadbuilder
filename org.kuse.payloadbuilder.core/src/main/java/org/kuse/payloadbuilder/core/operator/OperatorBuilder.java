package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.containsAny;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.kuse.payloadbuilder.core.operator.OperatorBuilderUtils.createGroupBy;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Catalog.OperatorData;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.AJoin;
import org.kuse.payloadbuilder.core.parser.ASelectVisitor;
import org.kuse.payloadbuilder.core.parser.AStatementVisitor;
import org.kuse.payloadbuilder.core.parser.Apply;
import org.kuse.payloadbuilder.core.parser.Apply.ApplyType;
import org.kuse.payloadbuilder.core.parser.AsteriskSelectItem;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionSelectItem;
import org.kuse.payloadbuilder.core.parser.Join;
import org.kuse.payloadbuilder.core.parser.Join.JoinType;
import org.kuse.payloadbuilder.core.parser.NestedSelectItem;
import org.kuse.payloadbuilder.core.parser.NestedSelectItem.Type;
import org.kuse.payloadbuilder.core.parser.Option;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.PopulateTableSource;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SelectStatement;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.Table;
import org.kuse.payloadbuilder.core.parser.TableFunction;
import org.kuse.payloadbuilder.core.parser.TableSource;
import org.kuse.payloadbuilder.core.parser.TableSourceJoined;

import gnu.trove.map.hash.THashMap;

/**
 * Builder that constructs a {@link Operator} and {@link Projection} for a {@link SelectStatement}
 */
public class OperatorBuilder extends ASelectVisitor<Void, OperatorBuilder.Context>
{
    private static final String BATCH_LIMIT = "batch_limit";
    private static final String BATCH_SIZE = "batch_size";
//    private static final String HASH_INNER = "hash_inner";
    private static final OperatorBuilder VISITOR = new OperatorBuilder();

    private static final BiFunction<String, TableAlias, RuntimeException> MULTIPLE_ALIAS_EXCEPTION = (alias,
            parent) -> new IllegalArgumentException("Alias " + alias + " is defined multiple times for parent: " + parent);

    private final CorrelatedDetector correlatedDetector = new CorrelatedDetector();

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
        private final Map<String, List<AnalyzePair>> pushDownPredicateByAlias = new THashMap<>();

        /** Sort items. Sent to catalog to for usage if supported. */
        private List<SortItem> sortItems;

        /** Acquire next unique node id */
        private int acquireNodeId()
        {
            int result = nodeId;
            nodeId++;
            return result;
        }

        /** Appends push down predicate for provided alias */
        private void appendPushDownPredicate(String alias, List<AnalyzePair> pairs)
        {
            if (pairs == null || pairs.isEmpty())
            {
                return;
            }
            pushDownPredicateByAlias.computeIfAbsent(alias, key -> new ArrayList<>()).addAll(pairs);
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
    public Void visit(Select select, Context context)
    {
        TableSourceJoined tsj = select.getFrom();
        Expression where = select.getWhere();

        if (tsj != null && tsj.getTableSource().getTable() != null)
        {
            TableSource ts = tsj.getTableSource();
            Set<String> availableAliases = new HashSet<>();
            availableAliases.add(ts.getAlias());
            tsj.getJoins().forEach(j -> availableAliases.add(j.getTableSource().getAlias()));

            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where, availableAliases);
            Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(ts.getAlias());
            context.appendPushDownPredicate(ts.getAlias(), pair.getKey());
            analyzeResult = pair.getValue();

            /* Push down predicates for joins */
            for (AJoin join : tsj.getJoins())
            {
                if (join instanceof Join && ((Join) join).getType() == JoinType.INNER)
                {
                    if (join.getTableSource() instanceof PopulateTableSource)
                    {
                        continue;
                    }
                    
                    Pair<List<AnalyzePair>, AnalyzeResult> p = analyzeResult.extractPushdownPairs(join.getTableSource().getAlias());
                    context.appendPushDownPredicate(join.getTableSource().getAlias(), p.getKey());
                    analyzeResult = p.getValue();
                }
            }

            where = analyzeResult.getPredicate();
        }

        Option batchLimitOption = null;
        int batchLimitId = -1;
        List<AJoin> joins = tsj != null ? tsj.getJoins() : emptyList();
        int joinSize = joins.size();
        // No need to batch when there is no joins
        if (joinSize > 0)
        {
            batchLimitOption = getOption(tsj.getTableSource(), BATCH_LIMIT);
        }

        context.sortItems = new ArrayList<>(select.getOrderBy());

        if (tsj != null)
        {
            tsj.getTableSource().accept(this, context);
            List<AnalyzePair> pairs = context.pushDownPredicateByAlias.remove(tsj.getTableSource().getAlias());
            if (pairs != null)
            {
                Expression predicate = new AnalyzeResult(pairs).getPredicate();
                // Gather columns from remaining predicate
                visit(predicate, context);
                if (predicate != null)
                {
                    context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(predicate));
                }
            }
        }

        if (batchLimitOption != null)
        {
            batchLimitId = context.acquireNodeId();
            context.operator = new BatchLimitOperator(batchLimitId, context.operator, batchLimitOption.getValueExpression());
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

        List<Expression> groupBys = select.getGroupBy();
        if (!select.getGroupBy().isEmpty())
        {
            groupBys.forEach(e -> visit(e, context));
            context.operator = createGroupBy(context.acquireNodeId(), tsj.getTableSource().getAlias(), groupBys, context.operator);
        }

        if (batchLimitOption != null)
        {
            context.operator = new BatchRepeatOperator(context.acquireNodeId(), batchLimitId, context.operator);
        }

        if (!context.sortItems.isEmpty())
        {
            context.sortItems.forEach(si -> si.accept(this, context));
            context.operator = new SortByOperator(context.acquireNodeId(), context.operator, new ExpressionRowComparator(context.sortItems));
        }

        if (select.getTopExpression() != null)
        {
            visit(select.getTopExpression(), context);
            context.operator = new TopOperator(context.acquireNodeId(), context.operator, select.getTopExpression());
        }

        List<String> projectionAliases = new ArrayList<>();
        List<Projection> projections = new ArrayList<>();
        select.getSelectItems().forEach(s ->
        {
            s.accept(this, context);
            projectionAliases.add(defaultIfBlank(s.getIdentifier(), ""));
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
            fromOperator = new SortByOperator(context.acquireNodeId(), fromOperator, new ExpressionRowComparator(nestedSelectItem.getOrderBy()));
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

    private static final Projection NO_OP_PROJECTION = (writer, context) ->
    {
    };

    @Override
    public Void visit(AsteriskSelectItem selectItem, Context context)
    {
        if (context.parent == null)
        {
            context.projection = NO_OP_PROJECTION;
            return null;
        }
        // Set asterisk columns on aliases
        if (selectItem.getAlias() != null)
        {
            if (context.parent.getAlias().equals(selectItem.getAlias()))
            {
                context.parent.setAsteriskColumns();
            }
            else
            {
                TableAlias childAlias = context.parent.getChildAlias(selectItem.getAlias());
                if (childAlias != null)
                {
                    childAlias.setAsteriskColumns();
                }
            }
        }
        else
        {
            context.parent.setAsteriskColumns();
            for (TableAlias alias : context.parent.getChildAliases())
            {
                alias.setAsteriskColumns();
            }
        }
        context.projection = selectItem;
        return null;
    }

    @Override
    protected void visit(Expression expression, Context context)
    {
        if (expression == null)
        {
            return;
        }
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
        if (where != null)
        {
            Set<String> availableAliases = getAvailableAliases(context, tableSource);
            tsj.getJoins().forEach(j -> availableAliases.add(j.getTableSource().getAlias()));
            // Analyze expression to see if there is push down candidates
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where, availableAliases);
            Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(tableSource.getAlias());
            context.appendPushDownPredicate(tableSource.getAlias(), pair.getKey());
            analyzeResult = pair.getValue();

            /* Push down predicates for joins */
            for (AJoin join : tsj.getJoins())
            {
                if (join instanceof Join && ((Join) join).getType() == JoinType.INNER)
                {
                    Pair<List<AnalyzePair>, AnalyzeResult> p = analyzeResult.extractPushdownPairs(join.getTableSource().getAlias());
                    context.appendPushDownPredicate(join.getTableSource().getAlias(), p.getKey());
                    analyzeResult = p.getValue();
                }
            }

            where = analyzeResult.getPredicate();
        }

        context.setParent = true;
        // Force populate table source alias
        context.alias = tableSource.getAlias();
        tsj.getTableSource().accept(this, context);
        context.alias = null;
        context.setParent = false;

        /* Is there any left push down that catalog
         * didn't used, then add a filter.
         * NOTE! This is done'e prior to joins to get the filter
         * as close to the source as possible. */
        List<AnalyzePair> pairs = context.pushDownPredicateByAlias.remove(tableSource.getAlias());
        if (pairs != null)
        {
            Expression predicate = new AnalyzeResult(pairs).getPredicate();
            visit(predicate, context);
            context.operator = new FilterOperator(context.acquireNodeId(), context.operator, new ExpressionPredicate(predicate));
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
        List<SortItem> sortBys = tableSource.getOrderBy();
        if (!sortBys.isEmpty())
        {
            sortBys.forEach(si -> si.accept(this, context));
            context.operator = new SortByOperator(context.acquireNodeId(), context.operator, new ExpressionRowComparator(sortBys));
        }

        // Restore values before leaving
        context.parent = parent;
        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        Pair<String, Catalog> pair = getCatalog(context, table.getCatalogAlias(), table.getToken());
        TableAlias alias = context.appendTableAlias(table.getTable(), table.getAlias(), null);
        int nodeId = context.acquireNodeId();

        List<AnalyzePair> predicatePairs = ObjectUtils.defaultIfNull(context.pushDownPredicateByAlias.get(table.getAlias()), emptyList());
        if (context.index != null)
        {
            context.operator = pair.getValue()
                    .getIndexOperator(new OperatorData(
                            context.session,
                            nodeId,
                            pair.getKey(),
                            alias,
                            predicatePairs,
                            emptyList(),
                            table.getOptions()),
                            context.index);
            context.index = null;
        }
        else
        {
            context.operator = pair.getValue()
                    .getScanOperator(new OperatorData(
                            context.session,
                            nodeId,
                            pair.getKey(),
                            alias,
                            predicatePairs,
                            context.sortItems,
                            table.getOptions()));
        }

        // No pairs left, remove from context
        if (predicatePairs != null && predicatePairs.isEmpty())
        {
            context.pushDownPredicateByAlias.remove(table.getAlias());
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
        context.operator = new TableFunctionOperator(context.acquireNodeId(), tableFunction.getCatalogAlias(), alias, functionInfo, tableFunction.getArguments());
        return null;
    }

    /**
     * Return a batch size option for provided table source (if any) Used to override default {@link Index#getBatchSize()} for a table
     **/
    private Option getOption(TableSource ts, String name)
    {
        return ts
                .getOptions()
                .stream()
                .filter(o -> name.equals(lowerCase(o.getOption().toString())))
                .findFirst()
                .orElse(null);
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

        boolean isCorrelated = correlatedDetector.isCorrelated(context.parent, join);

        Operator joinOperator = createJoin(
                context,
                logicalOperator,
                outer,
                join.getTableSource(),
                condition,
                emitEmptyOuterRows,
                isCorrelated);

        if (condition != null)
        {
            TableAlias parent = context.parent;
            // Set inner table alias into context before analyzing condition
            // This to be able to use alias less references pointing to the inner table
            // ie.
            // from tableA a
            // inner join tableB b
            //   on col = a.col    <------ col here refers to tableB
            //
            context.parent = context.parent.getChildAlias(join.getTableSource().getAlias());
            visit(condition, context);
            context.parent = parent;
        }
        context.operator = joinOperator;
    }

    private Set<String> getAvailableAliases(Context context, TableSource tableSource)
    {
        Set<String> availableAliases = new HashSet<>();
        TableAlias alias = context.parent;
        while (alias != null)
        {
            availableAliases.add(alias.getAlias());
            alias.getChildAliases().forEach(a -> availableAliases.add(a.getAlias()));
            alias = alias.getParent();
        }
        availableAliases.add(tableSource.getAlias());
        return availableAliases;
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
        String innerAlias = innerTableSource.getAlias();
        boolean populating = innerTableSource instanceof PopulateTableSource;
        List<Index> indices = emptyList();
        if (innerTableSource.getTable() != null)
        {
            Pair<String, Catalog> pair = getCatalog(context, innerTableSource.getCatalogAlias(), innerTableSource.getToken());
            indices = pair.getValue().getIndices(context.session, pair.getKey(), innerTableSource.getTable());
        }
        // TODO: If outer is ordered and no inner indices
        // then the inner could be created and after, check it's order for a potential
        // MergeJoin

        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(joinCondition, getAvailableAliases(context, innerTableSource));
        IndexOperatorFoundation foundation = new IndexOperatorFoundation(innerAlias, populating, indices, analyzeResult, emitEmptyOuterRows);
        Index index = foundation.index;
        Expression condition = foundation.condition.getPredicate();
        context.appendPushDownPredicate(innerAlias, foundation.pushDownPairs);

        /* No equi items in condition or a correlated query => NestedLoop */
        if (isCorrelated || !foundation.isEqui())
        {
            boolean hasIndex = index != null;
            context.index = index;
            innerTableSource.accept(this, context);
            if (context.index != null)
            {
                throw new RuntimeException("Index " + index + " should have been consumed by " + innerTableSource.getTable());
            }

            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

            // Cache inner operator if this is a regular nested loop
            if (!isCorrelated)
            {
                inner = new CachingOperator(context.acquireNodeId(), inner);
            }
            // If we have a correlated query and a inner index
            // wrap inner operator with an outer values operator to
            // let the inner utilize each nested loops outer rows index values
            else if (hasIndex)
            {
                inner = new OuterValuesOperator(context.acquireNodeId(), inner, foundation.outerValueExpressions);
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

        // No indices for inner -> HashJoin
        if (index == null)
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);
            // TODO: implement hash of inner and probe of outer
            // Option hashInner = getOption(innerTableSource, HASH_INNER);
            return new HashJoin(
                    context.acquireNodeId(),
                    logicalOperator,
                    outer,
                    inner,
                    // Collect outer hash expressions
                    new ExpressionHashFunction(foundation.outerValueExpressions),
                    // Collect inner hash expression
                    new ExpressionHashFunction(foundation.innerValueExpressions),
                    new ExpressionPredicate(condition),
                    DefaultRowMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        context.index = index;
        innerTableSource.accept(this, context);
        if (context.index != null)
        {
            throw new RuntimeException("Index " + index + " should have been consumed by " + innerTableSource.getTable());
        }
        Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

        constantalizeValueExtractors(foundation.outerValueExpressions, foundation.innerValueExpressions);
        ExpressionValuesExtractor outerValuesExtractor = new ExpressionValuesExtractor(foundation.outerValueExpressions);
        ExpressionValuesExtractor innerValuesExtractor = new ExpressionValuesExtractor(foundation.innerValueExpressions);

        // Get batch size option for provided table source (if any) Used to override default {@link Index#getBatchSize()} for a table
        Option batchSizeOption = getOption(innerTableSource, BATCH_SIZE);

        return new BatchHashJoin(
                context.acquireNodeId(),
                logicalOperator,
                outer,
                inner,
                outerValuesExtractor,
                innerValuesExtractor,
                condition != null ? new ExpressionPredicate(condition) : (ctx, row) -> true,
                DefaultRowMerger.DEFAULT,
                populating,
                emitEmptyOuterRows,
                index,
                batchSizeOption);

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

    /** Fetch index from provided equi pairs and indices list */
    private static Index getIndex(List<AnalyzePair> equiPairs, String alias, List<Index> indices)
    {
        /* Extract all references inner columns from equi items */
        Set<String> columnItems = equiPairs
                .stream()
                .map(pair -> pair.getColumn(alias))
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

        return index;
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
    private static void constantalizeValueExtractors(List<Expression> outerValueExpressions, List<Expression> innerValueExpressions)
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

    /** Wraps provided operator with push down predicate filter for provided alias **/
    private Operator wrapWithPushDown(Context context, Operator operator, String alias)
    {
        List<AnalyzePair> pushDownPairs = context.pushDownPredicateByAlias.remove(alias);
        /* Apply any pushdown filter */
        if (pushDownPairs != null)
        {
            Expression pushDownPredicate = new AnalyzeResult(pushDownPairs).getPredicate();
            visit(pushDownPredicate, context);
            return new FilterOperator(context.acquireNodeId(), operator, new ExpressionPredicate(pushDownPredicate));
        }
        return operator;
    }

    /** Fetch catalog and resulting qualifed name from provided table */
    private Pair<String, Catalog> getCatalog(Context context, String catalogAlias, Token token)
    {
        if (isBlank(catalogAlias))
        {
            Catalog catalog = context.session.getDefaultCatalog();
            if (catalog == null)
            {
                throw new ParseException("No default catalog set", token);
            }
            return Pair.of(context.session.getDefaultCatalogAlias(), catalog);
        }

        Catalog catalog = context.session.getCatalogRegistry().getCatalog(catalogAlias);
        if (catalog == null)
        {
            throw new ParseException("No catalog found with name " + catalogAlias, token);
        }

        return Pair.of(catalogAlias, catalog);
    }

    /** Class containing information needed to build a index operator */
    private static class IndexOperatorFoundation
    {
        List<Expression> outerValueExpressions;
        List<Expression> innerValueExpressions;
        AnalyzeResult condition;
        List<AnalyzePair> pushDownPairs;
        Index index;

        boolean isEqui()
        {
            return outerValueExpressions != null && outerValueExpressions.size() > 0;
        }

        IndexOperatorFoundation(
                String alias,
                boolean populate,
                List<Index> indices,
                AnalyzeResult analyzeResult,
                boolean emitEmptyOuterRows)
        {
            int size = analyzeResult.getPairs().size();
            List<AnalyzePair> equiItems = new ArrayList<>(size);
            List<AnalyzePair> leftOverEquiItems = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                AnalyzePair pair = analyzeResult.getPairs().get(i);
                if (pair.isEqui(alias))
                {
                    equiItems.add(pair);
                }
                else
                {
                    leftOverEquiItems.add(pair);
                }
            }

            if (equiItems.size() == 0)
            {
                Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(alias);
                pushDownPairs = pair.getKey();
                condition = pair.getValue();
                return;
            }

            /*
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
            */
            index = getIndex(equiItems, alias, indices);
            size = equiItems.size();
            outerValueExpressions = new ArrayList<>(size);
            innerValueExpressions = new ArrayList<>(size);
            Set<String> processedColumns = new HashSet<>();
            for (int i = 0; i < size; i++)
            {
                AnalyzePair pair = equiItems.get(i);
                boolean isPushdown = pair.isPushdown(alias);
                String column = pair.getColumn(alias);
                int columnIndex = 0;
                boolean isIndexPair = false;
                if (index != null && column != null)
                {
                    columnIndex = index.getColumns().indexOf(column);
                    if (columnIndex == -1)
                    {
                        leftOverEquiItems.add(pair);
                        continue;
                    }

                    if (!processedColumns.add(column))
                    {
                        // TODO: pick the best avaiable expression
                        // Ie. Expression#isConstant over any other
                        continue;
                    }

                    isIndexPair = true;
                }

                if (isIndexPair || !isPushdown)
                {
                    Pair<Expression, Expression> p = pair.getExpressionPair(alias);
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

                // Don't re-add pair if it's a index-pushdown item
                if (!(isIndexPair && pair.isPushdown(alias)))
                {
                    leftOverEquiItems.add(pair);
                }
            }

            // TODO: Analyze which sub aliases is invalid as push down
            // Ie. predicates outside of populating join that acceses sub aliases
            // cannot be pushed down
            AnalyzeResult result = new AnalyzeResult(leftOverEquiItems);

            if (populate)
            {
                condition = result;
                return;
            }
//            // No push down in LEFT JOINS
//            if (emitEmptyOuterRows)
//            {
//                condition = result;
//                return;
//            }

            Pair<List<AnalyzePair>, AnalyzeResult> pair = result.extractPushdownPairs(alias);
            pushDownPairs = pair.getKey();
            condition = pair.getValue();
        }
    }

    /**
     * Detects correlated queries by analyzing qualified expression references in child join to see if any aliases from outer context is referenced.
     */
    private class CorrelatedDetector extends AStatementVisitor<Void, Set<String>>
    {
        private final AExpressionVisitor<Void, Set<String>> expressionVisitor = new AExpressionVisitor<Void, Set<String>>()
        {
            @Override
            public Void visit(QualifiedReferenceExpression expression, Set<String> aliases)
            {
                String alias = expression.getQname().getAlias();
                if (alias != null)
                {
                    aliases.add(alias);
                }
                return null;
            }
        };

        @Override
        protected void visitExpression(Set<String> context, Expression expression)
        {
            expression.accept(expressionVisitor, context);
        }

        boolean isCorrelated(TableAlias alias, AJoin join)
        {
            TableAlias current = alias;
            Set<String> aliases = new HashSet<>();
            // Collect all current aliases
            // Including parents and parents child aliases
            while (current != null)
            {
                aliases.add(current.getAlias());
                current.getChildAliases().forEach(a -> aliases.add(a.getAlias()));
                current = current.getParent();
            }

            // If any of these are referenced in provided join then we have a
            // correlated query
            Set<String> joinAliases = new HashSet<>();
            join.getTableSource().accept(this, joinAliases);

            return containsAny(aliases, joinAliases);
        }
    }
}
