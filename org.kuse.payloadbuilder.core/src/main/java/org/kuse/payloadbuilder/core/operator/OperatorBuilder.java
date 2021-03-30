package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
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

import org.antlr.v4.runtime.Token;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Catalog.OperatorData;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.Index;
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
import org.kuse.payloadbuilder.core.parser.ColumnsVisitor;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionSelectItem;
import org.kuse.payloadbuilder.core.parser.Join;
import org.kuse.payloadbuilder.core.parser.Join.JoinType;
import org.kuse.payloadbuilder.core.parser.NestedSelectItem;
import org.kuse.payloadbuilder.core.parser.NestedSelectItem.Type;
import org.kuse.payloadbuilder.core.parser.Option;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SelectStatement;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SubQueryTableSource;
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
    private static final String POPULATE = "populate";

    //    private static final String HASH_INNER = "hash_inner";
    private static final OperatorBuilder VISITOR = new OperatorBuilder();

    private final CorrelatedDetector correlatedDetector = new CorrelatedDetector();

    /** Context used during visiting tree */
    static class Context
    {
        /** Counter for unique node ids */
        private int nodeId;

        private QuerySession session;
        private CatalogRegistry registry;

        /** Current table alias used when analyzing FROM-part in the query */
        private TableAlias currentTableAlias;

        /**
         * Current table aliases used when analyzing SELECT-items part in the query.
         *
         * <pre>
         * We need a set of aliases when analyzing the select items since we can have FROM
         * expressions that unions two or more aliases ie. FROM unionall(alias1, alias2)
         * then we need to resolve all items with context of a set of aliases
         * </pre>
         **/
        private Set<TableAlias> currentTableAliases;

        Map<TableAlias, Set<String>> columnsByAlias = new THashMap<>();

        /** Resulting operator */
        private Operator operator;

        /** Resulting projection */
        private Projection projection;

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
    }

    /** Create operator */
    public static Pair<Operator, Projection> create(QuerySession session, Select select)
    {
        Context context = new Context();
        context.session = session;
        context.registry = session.getCatalogRegistry();
        select.accept(VISITOR, context);
        context.columnsByAlias.entrySet().forEach(e ->
        {
            if (e.getKey() != null && e.getKey().getColumns().length == 0)
            {
                e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY));
            }
        });

        return Pair.of(context.operator, context.projection);
    }

    //CSOFF
    @Override
    //CSON
    public Void visit(Select select, Context context)
    {
        TableSourceJoined tsj = select.getFrom();
        Expression where = select.getWhere();

        if (tsj != null && tsj.getTableSource().getTable() != null)
        {
            TableSource ts = tsj.getTableSource();
            String alias = ts.getTableAlias().getAlias();
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where, ts.getTableAlias());
            Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(alias);
            context.appendPushDownPredicate(alias, pair.getKey());
            analyzeResult = pair.getValue();

            /* Push down predicates for joins */
            for (AJoin join : tsj.getJoins())
            {
                //                            left        inner       cross       outer
                //                is null     X           PUSH DOWN   PUSH DOWN   X
                //                is not null PUSH DOWN   PUSH DOWN   PUSH DOWN   PUSH DOWN

                boolean isNullAllowed = !((join instanceof Join && ((Join) join).getType() == JoinType.LEFT)
                    ||
                    (join instanceof Apply && ((Apply) join).getType() == ApplyType.OUTER));

                String joinAlias = join.getTableSource().getTableAlias().getAlias();
                Pair<List<AnalyzePair>, AnalyzeResult> p = analyzeResult.extractPushdownPairs(joinAlias, isNullAllowed);
                context.appendPushDownPredicate(joinAlias, p.getKey());
                analyzeResult = p.getValue();
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
            List<AnalyzePair> pairs = context.pushDownPredicateByAlias.remove(tsj.getTableSource().getTableAlias().getAlias());
            if (pairs != null)
            {
                Expression predicate = new AnalyzeResult(pairs).getPredicate();
                // Gather columns from remaining predicate
                visit(predicate, context);
                //CSOFF
                if (predicate != null)
                //CSON
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
            context.operator = createGroupBy(context.acquireNodeId(), groupBys, context.operator);
        }

        if (batchLimitOption != null)
        {
            context.operator = new BatchRepeatOperator(context.acquireNodeId(), batchLimitId, context.operator);
        }

        if (!context.sortItems.isEmpty())
        {
            context.sortItems.forEach(si -> si.accept(this, context));
            context.operator = new SortByOperator(context.acquireNodeId(), context.operator, new ExpressionTupleComparator(context.sortItems));
        }

        if (select.getTopExpression() != null)
        {
            visit(select.getTopExpression(), context);
            context.operator = new TopOperator(context.acquireNodeId(), context.operator, select.getTopExpression());
        }

        context.currentTableAlias = tsj != null ? tsj.getTableSource().getTableAlias() : null;

        // Before processing the select items set the collection alias property in context to mark
        // that this should be used
        context.currentTableAliases = context.currentTableAlias != null ? asSet(context.currentTableAlias) : emptySet();

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

    //CSOFF
    @Override
    //CSON
    public Void visit(NestedSelectItem nestedSelectItem, Context context)
    {
        Set<TableAlias> aliases = context.currentTableAliases;
        Operator fromOperator = null;
        Expression from = nestedSelectItem.getFrom();
        if (from != null)
        {
            fromOperator = new ExpressionOperator(context.acquireNodeId(), from);
            aliases = ColumnsVisitor.getColumnsByAlias(context.session, context.columnsByAlias, aliases, from);
        }

        List<String> projectionAliases = new ArrayList<>();
        List<Projection> projections = new ArrayList<>();
        boolean projectionsAdded = false;

        // Use found aliases and traverse select items, order, groupBy and where
        Set<TableAlias> parent = context.currentTableAliases;
        context.currentTableAliases = aliases;
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
        //        }

        if (nestedSelectItem.getWhere() != null)
        {
            fromOperator = new FilterOperator(context.acquireNodeId(), fromOperator, new ExpressionPredicate(nestedSelectItem.getWhere()));
        }

        if (!nestedSelectItem.getGroupBy().isEmpty())
        {
            fromOperator = createGroupBy(context.acquireNodeId(), nestedSelectItem.getGroupBy(), fromOperator);
        }

        if (!nestedSelectItem.getOrderBy().isEmpty())
        {
            fromOperator = new SortByOperator(context.acquireNodeId(), fromOperator, new ExpressionTupleComparator(nestedSelectItem.getOrderBy()));
        }

        if (nestedSelectItem.getType() == Type.ARRAY)
        {
            context.projection = new ArrayProjection(projections, fromOperator);
        }
        else
        {
            context.projection = new ObjectProjection(projectionAliases, projections, fromOperator);
        }
        context.currentTableAliases = parent;
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
    public Void visit(AsteriskSelectItem selectItem, Context context)
    {
        if (CollectionUtils.isEmpty(context.currentTableAliases))
        {
            context.projection = Projection.NO_OP_PROJECTION;
            return null;
        }

        List<Integer> tupleOrdinals = new ArrayList<>();
        // Process all alias in context for this select item
        for (TableAlias alias : context.currentTableAliases)
        {
            // Set asterisk columns on aliases
            if (selectItem.getAlias() != null)
            {
                if (alias.getAlias().equalsIgnoreCase(selectItem.getAlias()))
                {
                    tupleOrdinals.add(alias.getTupleOrdinal());
                    alias.setAsteriskColumns();
                }
                else
                {
                    // Try parents children
                    TableAlias tempAlias = alias.getChildAlias(selectItem.getAlias());
                    //CSOFF
                    if (tempAlias != null)
                    //CSON
                    {
                        tempAlias.setAsteriskColumns();
                        tupleOrdinals.add(tempAlias.getTupleOrdinal());
                        continue;
                    }
                    // Sibling
                    tempAlias = alias.getSiblingAlias(selectItem.getAlias());
                    //CSOFF
                    if (tempAlias != null)
                    //CSON
                    {
                        tempAlias.setAsteriskColumns();
                        tupleOrdinals.add(tempAlias.getTupleOrdinal());
                    }
                }
            }
            else
            {
                alias.setAsteriskColumns();
                for (TableAlias childAlias : alias.getChildAliases())
                {
                    childAlias.setAsteriskColumns();
                }
            }
        }

        if (selectItem.getAlias() != null && tupleOrdinals.isEmpty())
        {
            throw new ParseException("No alias found with name: " + selectItem.getAlias(), selectItem.getToken());
        }

        if (!tupleOrdinals.isEmpty())
        {
            selectItem.setAliasTupleOrdinals(tupleOrdinals);
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

        // In multi alias mode => processing select items
        if (context.currentTableAliases != null)
        {
            ColumnsVisitor.getColumnsByAlias(context.session, context.columnsByAlias, context.currentTableAliases, expression);
        }
        else
        {
            ColumnsVisitor.getColumnsByAlias(context.session, context.columnsByAlias, context.currentTableAlias, expression);
        }
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
    public Void visit(SubQueryTableSource tableSource, Context context)
    {
        TableSourceJoined tsj = tableSource.getTableSourceJoined();
        TableSource ts = tsj.getTableSource();
        TableAlias tableAlias = ts.getTableAlias();
        String alias = tableAlias.getAlias();

        TableAlias prevCurrentTableAlias = context.currentTableAlias;

        Expression where = tableSource.getWhere();
        if (where != null)
        {
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where, ts.getTableAlias());
            Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(alias);
            context.appendPushDownPredicate(alias, pair.getKey());
            analyzeResult = pair.getValue();

            /* Push down predicates for joins */
            for (AJoin join : tsj.getJoins())
            {
                String joinAlias = join.getTableSource().getTableAlias().getAlias();
                Pair<List<AnalyzePair>, AnalyzeResult> p = analyzeResult.extractPushdownPairs(joinAlias);
                context.appendPushDownPredicate(joinAlias, p.getKey());
                analyzeResult = p.getValue();
            }

            where = analyzeResult.getPredicate();
        }

        tsj.getTableSource().accept(this, context);

        /* Is there any left push down that catalog
         * didn't used, then add a filter.
         * NOTE! This is done'e prior to joins to get the filter
         * as close to the source as possible. */
        List<AnalyzePair> pairs = context.pushDownPredicateByAlias.remove(alias);
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
            context.operator = createGroupBy(context.acquireNodeId(), groupBys, context.operator);
        }
        List<SortItem> sortBys = tableSource.getOrderBy();
        if (!sortBys.isEmpty())
        {
            sortBys.forEach(si -> si.accept(this, context));
            context.operator = new SortByOperator(context.acquireNodeId(), context.operator, new ExpressionTupleComparator(sortBys));
        }

        //        context.operator = new SubQueryOperator(context.operator, tableSource.getTableAlias().getAlias());

        // Restore prev alias
        context.currentTableAlias = prevCurrentTableAlias;

        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        Pair<String, Catalog> pair = getCatalog(context, table.getCatalogAlias(), table.getToken());
        int nodeId = context.acquireNodeId();
        context.currentTableAlias = table.getTableAlias();
        String alias = table.getTableAlias().getAlias();
        List<AnalyzePair> predicatePairs = ObjectUtils.defaultIfNull(context.pushDownPredicateByAlias.get(alias), emptyList());
        if (context.index != null)
        {
            context.operator = pair.getValue()
                    .getIndexOperator(new OperatorData(
                            context.session,
                            nodeId,
                            pair.getKey(),
                            table.getTableAlias(),
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
                            table.getTableAlias(),
                            predicatePairs,
                            context.sortItems,
                            table.getOptions()));
        }

        // No pairs left, remove from context
        if (predicatePairs != null && predicatePairs.isEmpty())
        {
            context.pushDownPredicateByAlias.remove(alias);
        }
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        context.currentTableAlias = tableFunction.getTableAlias();
        context.currentTableAlias.setColumns(functionInfo.getColumns());
        tableFunction.getArguments().forEach(a -> visit(a, context));
        context.operator = new TableFunctionOperator(context.acquireNodeId(), tableFunction.getCatalogAlias(), tableFunction.getTableAlias(), functionInfo, tableFunction.getArguments());
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

        boolean isCorrelated = correlatedDetector.isCorrelated(context, join);

        Operator joinOperator = createJoin(
                context,
                logicalOperator,
                outer,
                join.getTableSource(),
                condition,
                emitEmptyOuterRows,
                isCorrelated);
        context.operator = joinOperator;
    }

    //CSOFF
    private Operator createJoin(
            //CSON
            Context context,
            String logicalOperator,
            Operator outer,
            TableSource innerTableSource,
            final Expression joinCondition,
            boolean emitEmptyOuterRows,
            boolean isCorrelated)
    {
        String innerAlias = innerTableSource.getTableAlias().getAlias();
        boolean populating = getOption(innerTableSource, POPULATE) != null;
        String catalogAlias = innerTableSource.getCatalogAlias();
        QualifiedName table = innerTableSource.getTable();
        List<Index> indices = emptyList();

        // Sub query, use the inner table sources table and catalog when resolving indices
        if (innerTableSource instanceof SubQueryTableSource)
        {
            SubQueryTableSource subQuery = (SubQueryTableSource) innerTableSource;
            catalogAlias = subQuery.getTableSourceJoined().getTableSource().getCatalogAlias();
            table = subQuery.getTableSourceJoined().getTableSource().getTable();
        }

        if (table != null)
        {
            Pair<String, Catalog> pair = getCatalog(context, catalogAlias, innerTableSource.getToken());
            indices = pair.getValue().getIndices(context.session, pair.getKey(), table);
        }
        // TODO: If outer is ordered and no inner indices
        // then the inner could be created and after, check it's order for a potential
        // MergeJoin

        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(joinCondition, innerTableSource.getTableAlias());
        IndexOperatorFoundation foundation = new IndexOperatorFoundation(innerAlias, indices, analyzeResult);
        Index index = foundation.index;
        Expression condition = foundation.condition.getPredicate();
        // This is a sub query => assign push down to inner table source alias
        // else the inner table source won't pick up the pushdown and we end up
        // with 2 nested filter operators
        if (innerTableSource instanceof SubQueryTableSource)
        {
            String alias = ((SubQueryTableSource) innerTableSource).getTableSourceJoined().getTableSource().getTableAlias().getAlias();

            // Move any previous push downs from sub query alias to inner table alias
            List<AnalyzePair> pairs = context.pushDownPredicateByAlias.remove(innerAlias);
            context.appendPushDownPredicate(alias, pairs);
            context.appendPushDownPredicate(alias, foundation.pushDownPairs);
        }
        else
        {
            context.appendPushDownPredicate(innerAlias, foundation.pushDownPairs);
        }
        context.currentTableAlias = innerTableSource.getTableAlias();
        visit(condition, context);

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

            Operator inner = wrapWithPushDown(context, context.operator, innerTableSource.getTableAlias());

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
                    DefaultTupleMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        // No indices for inner -> HashJoin
        if (index == null)
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerTableSource.getTableAlias());
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
                    DefaultTupleMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        context.index = index;
        innerTableSource.accept(this, context);
        if (context.index != null)
        {
            throw new RuntimeException("Index " + index + " should have been consumed by " + innerTableSource.getTable());
        }
        Operator inner = wrapWithPushDown(context, context.operator, innerTableSource.getTableAlias());

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
                DefaultTupleMerger.DEFAULT,
                populating,
                emitEmptyOuterRows,
                index,
                batchSizeOption);
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
                    .filter(i -> i.getColumns() == Index.ALL_COLUMNS || columnItems.containsAll(i.getColumns()))
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
    private Operator wrapWithPushDown(Context context, Operator operator, TableAlias tableAlias)
    {
        List<AnalyzePair> pushDownPairs = context.pushDownPredicateByAlias.remove(tableAlias.getAlias());
        /* Apply any pushdown filter */
        if (pushDownPairs != null)
        {
            Expression pushDownPredicate = new AnalyzeResult(pushDownPairs).getPredicate();
            context.currentTableAlias = tableAlias;
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
            Catalog catalog = context.registry.getDefaultCatalog();
            if (catalog == null)
            {
                throw new ParseException("No default catalog set", token);
            }
            return Pair.of(context.registry.getDefaultCatalogAlias(), catalog);
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

        //CSOFF
        IndexOperatorFoundation(
                //CSON
                String alias,
                List<Index> indices,
                AnalyzeResult analyzeResult)
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
            List<String> indexColumns = null;
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
                    if (index.getColumns() == Index.ALL_COLUMNS)
                    {
                        //CSOFF
                        if (indexColumns == null)
                        //CSON
                        {
                            indexColumns = new ArrayList<>(size);
                        }
                        columnIndex = indexColumns.size();
                        indexColumns.add(column);
                    }
                    else
                    {
                        columnIndex = index.getColumns().indexOf(column);
                        //CSOFF
                        if (columnIndex == -1)
                        //CSON
                        {
                            leftOverEquiItems.add(pair);
                            continue;
                        }
                        //CSOFF
                        if (!processedColumns.add(column))
                        //CSON
                        {
                            // TODO: pick the best available expression
                            // Ie. Expression#isConstant over any other
                            continue;
                        }
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

            // Create a new index with current columns
            if (indexColumns != null)
            {
                index = new Index(index.getTable(), indexColumns, index.getBatchSize());
            }

            AnalyzeResult result = new AnalyzeResult(leftOverEquiItems);
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

        boolean isCorrelated(Context context, AJoin join)
        {
            TableAlias current = context.currentTableAlias;
            Set<String> aliases = new HashSet<>();
            // Collect all current aliases
            // Including parents
            while (current != null)
            {
                // Don't collect ROOT
                if (current.getParent() != null)
                {
                    aliases.add(current.getAlias());
                }
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
