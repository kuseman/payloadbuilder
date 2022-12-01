package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static org.apache.commons.collections4.CollectionUtils.containsAny;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static se.kuseman.payloadbuilder.core.operator.OperatorBuilderUtils.createGroupBy;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Catalog.OperatorData;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.codegen.CodeGenerator;
import se.kuseman.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.parser.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.parser.AJoin;
import se.kuseman.payloadbuilder.core.parser.ASelectVisitor;
import se.kuseman.payloadbuilder.core.parser.AStatementVisitor;
import se.kuseman.payloadbuilder.core.parser.Apply;
import se.kuseman.payloadbuilder.core.parser.Apply.ApplyType;
import se.kuseman.payloadbuilder.core.parser.AsteriskSelectItem;
import se.kuseman.payloadbuilder.core.parser.Expression;
import se.kuseman.payloadbuilder.core.parser.ExpressionSelectItem;
import se.kuseman.payloadbuilder.core.parser.Join;
import se.kuseman.payloadbuilder.core.parser.Join.JoinType;
import se.kuseman.payloadbuilder.core.parser.Option;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import se.kuseman.payloadbuilder.core.parser.Select;
import se.kuseman.payloadbuilder.core.parser.SelectItem;
import se.kuseman.payloadbuilder.core.parser.SelectStatement;
import se.kuseman.payloadbuilder.core.parser.SubQueryExpression;
import se.kuseman.payloadbuilder.core.parser.SubQueryTableSource;
import se.kuseman.payloadbuilder.core.parser.Table;
import se.kuseman.payloadbuilder.core.parser.TableFunction;
import se.kuseman.payloadbuilder.core.parser.TableSource;
import se.kuseman.payloadbuilder.core.parser.TableSourceJoined;
import se.kuseman.payloadbuilder.core.parser.UnresolvedSubQueryExpression;

/**
 * Builder that constructs a {@link Operator} and {@link Projection} for a {@link SelectStatement}
 */
public class OperatorBuilder extends ASelectVisitor<Void, OperatorBuilder.Context>
{
    private static final String BATCH_LIMIT = "batch_limit";
    private static final String BATCH_SIZE = "batch_size";
    private static final String POPULATE = "populate";

    private static final CodeGenerator CODE_GENERATOR = new CodeGenerator();

    // private static final String HASH_INNER = "hash_inner";
    private static final OperatorBuilder VISITOR = new OperatorBuilder();

    private final CorrelatedDetector correlatedDetector = new CorrelatedDetector();

    /** Context used during visiting tree */
    static class Context
    {
        /** Counter for unique node ids */
        private int nodeId;
        private QuerySession session;
        /** Resulting operator */
        private Operator operatorResult;
        /** Resulting select items */
        private Projection projectionResult;
        /** IndexPredicate if any that should be picked up when creating table operator */
        private IndexPredicate indexPredicate;
        /** Predicate pushed down from join to table source */
        private final Map<String, List<IAnalyzePair>> pushDownPredicateByAlias = new HashMap<>();
        /** Sort items. Sent to catalog to for usage if supported. */
        private List<ISortItem> sortItems = emptyList();
        /**
         * Analyze mode. If true a intercepting operator will be inserted in front of each operator that analyzes and performs measurement etc.
         */
        private boolean analyze;
        /** Flag to mark if the visitor is inside a root select or below */
        private boolean rootSelect = true;
        /** Flag to indicate if we should generate code for root projection */
        private boolean codeGenerateRootProjection = true;

        /**
         * Flag to indicate if we are inside a sub query expression, if so then a {@link SubQueryExpressionOperator} should wrap the select operator
         */
        private boolean insideSubQueryExpression;

        /** Acquire next unique node id */
        private int acquireNodeId()
        {
            return nodeId++;
        }

        /** Set current operator */
        private void setOperator(Operator operator)
        {
            if (analyze
                    && !(operator instanceof AnalyzeOperator))
            {
                this.operatorResult = new AnalyzeOperator(acquireNodeId(), operator);
            }
            else
            {
                this.operatorResult = operator;
            }
        }

        /** Wrap operator if needed */
        private Operator wrap(Operator operator)
        {
            if (analyze
                    && !(operator instanceof AnalyzeOperator))
            {
                return new AnalyzeOperator(acquireNodeId(), operator);
            }

            return operator;
        }

        /** Get current operator */
        private Operator getOperator()
        {
            return operatorResult;
        }

        /** Appends push down predicate for provided alias */
        private void appendPushDownPredicate(String alias, List<IAnalyzePair> pairs)
        {
            if (pairs == null
                    || pairs.isEmpty())
            {
                return;
            }
            pushDownPredicateByAlias.computeIfAbsent(alias, key -> new ArrayList<>())
                    .addAll(pairs);
        }
    }

    /** Create operator */
    public static BuildResult create(QuerySession session, Select select)
    {
        return create(session, select, false);
    }

    /** Create operator */
    public static BuildResult create(QuerySession session, Select select, boolean analyze)
    {
        Context context = new Context();
        context.analyze = analyze;
        context.session = session;
        select.accept(VISITOR, context);

        return new BuildResult(context.getOperator(), context.projectionResult);
    }

    /** Result of operator building */
    public static class BuildResult
    {
        private final Operator operator;
        private final Projection projection;

        BuildResult(Operator operator, Projection projection)
        {
            this.operator = operator;
            this.projection = projection;
        }

        public Operator getOperator()
        {
            return operator;
        }

        public Projection getProjection()
        {
            return projection;
        }
    }

    @Override
    protected void visit(Expression expression, Context context)
    {
        throw new RuntimeException("NONO");
    }

    /**
     * Core visitor for select clause
     *
     * <pre>
     *
     * select *
     * from
     * (
     *      select col
     *      from table
     * ) x
     * </pre>
     */
    @Override
    public Void visit(Select select, Context context)
    {
        TableSourceJoined tsj = select.getFrom();
        // CSOFF
        Expression where = pushDownPredicatesToJoins(tsj, select.getWhere(), context);
        // CSON

        TableAlias tableAlias = tsj != null ? tsj.getTableSource()
                .getTableAlias()
                : null;

        Option batchLimitOption = null;
        int batchLimitId = -1;
        List<AJoin> joins = tsj != null ? tsj.getJoins()
                : emptyList();
        int joinSize = joins.size();
        // No need to batch when there is no joins
        if (joinSize > 0)
        {
            batchLimitOption = getOption(tsj.getTableSource(), BATCH_LIMIT);
        }

        // TODO: This thing might need some rewrite, it's a very loose coupling
        // between this selects order by and which eventual catalogs table source that will pick it
        context.sortItems = new ArrayList<>(select.getOrderBy());
        // CSOFF
        List<ISortItem> sortItems = context.sortItems;
        // CSON

        if (tsj != null)
        {
            String alias = tsj.getTableSource()
                    .getTableAlias()
                    .getAlias();
            tsj.getTableSource()
                    .accept(this, context);
            List<IAnalyzePair> pairs = context.pushDownPredicateByAlias.remove(alias);
            if (pairs != null)
            {
                Expression predicate = new AnalyzeResult(pairs).getPredicate();
                // CSOFF
                if (predicate != null)
                // CSON
                {
                    context.setOperator(new FilterOperator(context.acquireNodeId(), context.getOperator(), createPredicate(context.session, predicate)));
                }
            }
        }
        else
        {
            // Set a no op operator if we don't have any table source
            // to avoid NPE's etc.
            context.setOperator(getEmptyTableSourceOperator(context));
        }

        if (context.insideSubQueryExpression)
        {
            // Use the linked alias if any
            TableAlias link = tableAlias != null ? ObjectUtils.defaultIfNull(tableAlias.getLinkAlias(), tableAlias)
                    : null;
            context.setOperator(new SubQueryExpressionOperator(context.acquireNodeId(), link, context.getOperator()));
        }

        if (batchLimitOption != null)
        {
            batchLimitId = context.acquireNodeId();
            context.setOperator(new BatchLimitOperator(batchLimitId, context.getOperator(), batchLimitOption.getValueExpression()));
        }

        for (int i = 0; i < joinSize; i++)
        {
            AJoin join = joins.get(i);
            join.accept(this, context);
        }

        // Always use select items from context cause these ones are aggregated in case of sub queries etc.
        List<SelectItem> selectItems = new ArrayList<>(select.getSelectItems());

        // Resolve sub query expressions
        processSubQueryExpressions(context, selectItems, select.getComputedExpressions());

        // Computed columns must come AFTER group by operator
        if (select.getGroupBy()
                .isEmpty())
        {
            pullUpComputedColumns(context, tableAlias, select.getComputedExpressions());
        }

        if (where != null)
        {
            context.setOperator(new FilterOperator(context.acquireNodeId(), context.getOperator(), new ExpressionPredicate(where)));
        }

        if (!select.getGroupBy()
                .isEmpty())
        {
            context.setOperator(createGroupBy(context.acquireNodeId(), select.getGroupBy(), createOrdinalValuesFactory(context.session, select.getGroupBy()), context.getOperator()));
            pullUpComputedColumns(context, tableAlias, select.getComputedExpressions());
        }

        if (batchLimitOption != null)
        {
            context.setOperator(new BatchRepeatOperator(context.acquireNodeId(), batchLimitId, context.getOperator()));
        }

        if (!sortItems.isEmpty())
        {
            context.setOperator(new OrderByOperator(context.acquireNodeId(), context.getOperator(), new ExpressionTupleComparator(sortItems)));
        }

        if (select.getTopExpression() != null)
        {
            context.setOperator(new TopOperator(context.acquireNodeId(), context.getOperator(), select.getTopExpression()));
        }

        if (context.rootSelect)
        {
            List<String> projectionAliases = new ArrayList<>();
            List<Projection> projections = new ArrayList<>();

            selectItems.forEach(s ->
            {
                projectionAliases.add(s.getIdentifier());
                projections.add(createProjection(context, s));
            });

            context.projectionResult = createRootProjection(context, new RootProjection(projectionAliases, projections));
            if (context.analyze)
            {
                context.projectionResult = new AnalyzeProjection(context.acquireNodeId(), context.projectionResult);
            }
        }

        return null;
    }

    @Override
    public Void visit(Join join, Context context)
    {
        join(context, (join.getType() == JoinType.LEFT ? "LEFT"
                : "INNER") + " JOIN", join, join.getCondition(), join.getType() == JoinType.LEFT);
        return null;
    }

    @Override
    public Void visit(Apply apply, Context context)
    {
        join(context, (apply.getType() == ApplyType.OUTER ? "OUTER"
                : "CROSS") + " APPLY", apply, null, apply.getType() == ApplyType.OUTER);
        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        if (table.isTempTable())
        {
            context.setOperator(new TemporaryTableScanOperator(context.acquireNodeId(), table));
            return null;
        }

        Pair<String, Catalog> pair = getCatalog(context, table.getCatalogAlias(), table.getToken());
        int nodeId = context.acquireNodeId();
        String alias = table.getTableAlias()
                .getAlias();
        List<IAnalyzePair> predicatePairs = ObjectUtils.defaultIfNull(context.pushDownPredicateByAlias.get(alias), emptyList());
        if (context.indexPredicate != null)
        {
            Operator catalogOperator = pair.getValue()
                    .getIndexOperator(new OperatorData(context.session, nodeId, pair.getKey(), table.getTableAlias(), predicatePairs, emptyList()), context.indexPredicate);

            if (catalogOperator == null)
            {
                throw new OperatorException("No operator returned from catalog " + pair.getKey() + " for table " + table.getTable());
            }

            context.setOperator(catalogOperator);
            context.indexPredicate = null;
        }
        else
        {
            Operator catalogOperator = pair.getValue()
                    .getScanOperator(new OperatorData(context.session, nodeId, pair.getKey(), table.getTableAlias(), predicatePairs, context.sortItems));

            if (catalogOperator == null)
            {
                throw new OperatorException("No operator returned from catalog " + pair.getKey() + " for table " + table.getTable());
            }

            context.setOperator(catalogOperator);
        }

        // No pairs left, remove from context
        if (predicatePairs != null
                && predicatePairs.isEmpty())
        {
            context.pushDownPredicateByAlias.remove(alias);
        }
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        context.setOperator(new TableFunctionOperator(context.acquireNodeId(), tableFunction.getCatalogAlias(), tableFunction.getTableAlias(), functionInfo, tableFunction.getArguments()));
        return null;
    }

    @Override
    public Void visit(SubQueryTableSource tableSource, Context context)
    {
        boolean prevCodeGenerateRootProjection = context.codeGenerateRootProjection;
        context.codeGenerateRootProjection = false;
        super.visit(tableSource, context);
        context.codeGenerateRootProjection = prevCodeGenerateRootProjection;
        return null;
    }

    /**
     * Processes sub query expressions and resolves those by building an operator and projection
     */
    private void processSubQueryExpressions(Context context, List<SelectItem> selectItems, List<Expression> computedExpressions)
    {
        // CSOFF
        Operator operatorResult = context.operatorResult;
        boolean isRootSelect = context.rootSelect;
        // CSON

        // Find and process select items who is UnresolvedSubQueryExpression's
        int size = selectItems.size();
        for (int i = 0; i < size; i++)
        {
            SelectItem item = selectItems.get(i);
            if (item instanceof ExpressionSelectItem
                    && ((ExpressionSelectItem) item).getExpression() instanceof UnresolvedSubQueryExpression)
            {
                UnresolvedSubQueryExpression subQueryExpression = (UnresolvedSubQueryExpression) ((ExpressionSelectItem) item).getExpression();
                SubQueryExpression resolvedExpression = resolveSubQueryExpression(context, subQueryExpression);
                selectItems.set(i, new ExpressionSelectItem(resolvedExpression, item.isEmptyIdentifier(), item.getIdentifier(), null, item.getToken()));
            }
        }

        // Process computed sub query expressions
        size = computedExpressions.size();
        for (int i = 0; i < size; i++)
        {
            Expression e = computedExpressions.get(i);
            if (e instanceof UnresolvedSubQueryExpression)
            {
                computedExpressions.set(i, resolveSubQueryExpression(context, (UnresolvedSubQueryExpression) e));
            }
        }

        context.rootSelect = isRootSelect;
        context.operatorResult = operatorResult;
    }

    private SubQueryExpression resolveSubQueryExpression(Context context, UnresolvedSubQueryExpression expression)
    {
        // We need a clean operator for each sub query expression
        context.operatorResult = null;
        context.rootSelect = true;
        // CSOFF
        boolean prevCodeGenerateRootProjection = context.codeGenerateRootProjection;
        // CSON
        context.codeGenerateRootProjection = false;
        boolean prevInsideSubQueryExpression = context.insideSubQueryExpression;

        Select select = expression.getSelectStatement()
                .getSelect();

        context.insideSubQueryExpression = true;
        // Visit
        select.accept(this, context);

        context.insideSubQueryExpression = prevInsideSubQueryExpression;
        context.codeGenerateRootProjection = prevCodeGenerateRootProjection;

        // Unwrap analyze node if any
        if (context.projectionResult instanceof AnalyzeProjection)
        {
            context.projectionResult = ((AnalyzeProjection) context.projectionResult).getTarget();
        }
        RootProjection rootProjection = (RootProjection) context.projectionResult;
        return new SubQueryExpression(context.operatorResult, rootProjection.getColumns(), rootProjection.getProjections(), select.getForOutput());
    }

    private Projection createProjection(Context context, SelectItem item)
    {
        Projection result = null;
        if (item instanceof ExpressionSelectItem)
        {
            result = new ExpressionProjection(((ExpressionSelectItem) item).getExpression());
        }
        else if (item instanceof AsteriskSelectItem)
        {
            result = new AsteriskProjection(ArrayUtils.toPrimitive(((AsteriskSelectItem) item).getAliasTupleOrdinals()
                    .toArray(ArrayUtils.EMPTY_INTEGER_OBJECT_ARRAY)));
        }

        if (result == null)
        {
            throw new IllegalArgumentException("Unknown select item type: " + item.getClass());
        }

        if (context.analyze)
        {
            return new AnalyzeProjection(context.acquireNodeId(), result);
        }

        return result;
    }

    /**
     * Pushes down predicates to joins if applicable
     *
     * <pre>
     *
     * ie
     *
     * select *
     * from tableA a
     * inner join tableB b
     *   on ...
     * inner join tableC c
     *   on ...
     * where b.col > 10             <--- these two should be pushed down
     * and c.col2 = 'type'          <--- to their corresponding operator
     * </pre>
     */
    private Expression pushDownPredicatesToJoins(TableSourceJoined tsj, Expression where, Context context)
    {
        if (tsj == null
                || tsj.getTableSource()
                        .getTable() == null)
        {
            return where;
        }

        TableSource ts = tsj.getTableSource();
        String alias = ts.getTableAlias()
                .getAlias();
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where, ts.getTableAlias());
        Pair<List<IAnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(alias);
        context.appendPushDownPredicate(alias, pair.getKey());
        analyzeResult = pair.getValue();

        /* Push down predicates for joins */
        for (AJoin join : tsj.getJoins())
        {
            // left inner cross outer
            // is null X PUSH DOWN PUSH DOWN X
            // is not null PUSH DOWN PUSH DOWN PUSH DOWN PUSH DOWN

            boolean isNullAllowed = !((join instanceof Join
                    && ((Join) join).getType() == JoinType.LEFT)
                    || (join instanceof Apply
                            && ((Apply) join).getType() == ApplyType.OUTER));

            String joinAlias = join.getTableSource()
                    .getTableAlias()
                    .getAlias();
            Pair<List<IAnalyzePair>, AnalyzeResult> p = analyzeResult.extractPushdownPairs(joinAlias, isNullAllowed);
            context.appendPushDownPredicate(joinAlias, p.getKey());
            analyzeResult = p.getValue();
        }

        return analyzeResult.getPredicate();
    }

    /**
     * <pre>
     * Pulls up all expressions select items is needed by sort items
     * or added in a sub query
     * This to be able to sort by expression items that would otherwise be invisible to
     * the sort operator.
     *
     * Also adjusts sort items with ordinal referenced columns like "ORDER BY 1"
     *
     *   Ex.
     *
     *   select col, col * 2 newCol
     *   from table
     *   order by newCol, 1
     *
     *   Pre Projection: col, col * 2 newCol
     *   Pre operator tree
     *     sort(newCol, 1)         <-- newCol and col does not exist here
     *       scan(table)
     *
     *   Post Projection: col, newCol
     *   Post operator tree
     *     sort(newCol)
     *       computedColumns (newCol: col * 2)
     *         scan(table)
     * </pre>
     *
     * @param tableAlias The alias the the computed columns should belong to
     **/
    // CSOFF
    private void pullUpComputedColumns(Context context, TableAlias tableAlias, List<Expression> computedExpressions)
    // CSON
    {
        if (tableAlias == null
                || computedExpressions.size() <= 0)
        {
            return;
        }

        int tupleOrdinal = tableAlias.getTupleOrdinal();
        context.setOperator(new ComputedColumnsOperator(context.acquireNodeId(), tupleOrdinal, context.getOperator(), createOrdinalValuesFactory(context.session, computedExpressions)));
    }

    /**
     * Return a batch size option for provided table source (if any) Used to override default {@link Index#getBatchSize()} for a table
     **/
    private Option getOption(TableSource ts, String name)
    {
        return ts.getOptions()
                .stream()
                .filter(o -> name.equals(lowerCase(o.getOption()
                        .toDotDelimited())))
                .findFirst()
                .orElse(null);
    }

    private void join(Context context, String logicalOperator, AJoin join, Expression condition, boolean emitEmptyOuterRows)
    {
        Operator outer = context.getOperator();
        context.setOperator(null);

        boolean isCorrelated = correlatedDetector.isCorrelated(join);

        Operator joinOperator = createJoin(context, logicalOperator, outer, join.getTableSource(), condition, emitEmptyOuterRows, isCorrelated);
        context.setOperator(joinOperator);
    }

    // CSOFF
    private Operator createJoin(
            // CSON
            Context context, String logicalOperator, Operator outer, TableSource innerTableSource, final Expression joinCondition, boolean emitEmptyOuterRows, boolean isCorrelated)
    {
        String innerAlias = innerTableSource.getTableAlias()
                .getAlias();
        boolean populating = getOption(innerTableSource, POPULATE) != null;
        String catalogAlias = innerTableSource.getCatalogAlias();
        QualifiedName table = innerTableSource.getTable();
        List<Index> indices = emptyList();
        int compositeTupleCountOnLevel = innerTableSource.getTableAlias()
                .getTupleCountCountOnLevel();

        // Sub query, use the inner table sources table and catalog when resolving indices
        if (innerTableSource instanceof SubQueryTableSource)
        {
            // TODO: needs some refactoring when doing nested unions etc.
            SubQueryTableSource subQuery = (SubQueryTableSource) innerTableSource;
            if (subQuery.getSelect()
                    .getFrom() != null)
            {
                catalogAlias = subQuery.getSelect()
                        .getFrom()
                        .getTableSource()
                        .getCatalogAlias();
                table = subQuery.getSelect()
                        .getFrom()
                        .getTableSource()
                        .getTable();
            }
        }

        if (table != null)
        {
            Pair<String, Catalog> pair = getCatalog(context, catalogAlias, innerTableSource.getToken());
            indices = pair.getValue()
                    .getIndices(context.session, pair.getKey(), table);
        }
        // TODO: If outer is ordered and no inner indices
        // then the inner could be created and after, check it's order for a potential
        // MergeJoin

        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(joinCondition, innerTableSource.getTableAlias());
        IndexOperatorFoundation foundation = new IndexOperatorFoundation(innerAlias, indices, analyzeResult);
        IndexPredicate indexPredicate = foundation.indexPredicate;
        Expression condition = foundation.condition.getPredicate();
        // This is a sub query => assign push down to inner table source alias
        // else the inner table source won't pick up the pushdown and we end up
        // with 2 nested filter operators
        if (innerTableSource instanceof SubQueryTableSource)
        {
            SubQueryTableSource subQuery = (SubQueryTableSource) innerTableSource;

            if (subQuery.getSelect()
                    .getFrom() != null)
            {
                // TODO: needs some refactoring when doing nested unions etc.
                String alias = subQuery.getSelect()
                        .getFrom()
                        .getTableSource()
                        .getTableAlias()
                        .getAlias();

                // Move any previous push downs from sub query alias to inner table alias
                List<IAnalyzePair> pairs = context.pushDownPredicateByAlias.remove(innerAlias);
                context.appendPushDownPredicate(alias, pairs);
                context.appendPushDownPredicate(alias, foundation.pushDownPairs);
            }
        }
        else
        {
            context.appendPushDownPredicate(innerAlias, foundation.pushDownPairs);
        }

        Predicate<ExecutionContext> predicate = createPredicate(context.session, condition);

        /* No equi items in condition or a correlated query => NestedLoop */
        if (isCorrelated
                || !foundation.isEqui())
        {
            context.indexPredicate = indexPredicate;
            innerTableSource.accept(this, context);
            if (context.indexPredicate != null)
            {
                throw new RuntimeException("Index " + indexPredicate.getIndex() + " should have been consumed by " + innerTableSource.getTable());
            }

            boolean hasIndex = indexPredicate != null;
            Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());

            // Cache inner operator if this is a regular nested loop
            // and it's not an un-filtered temp table.
            if (!isCorrelated
                    && !(inner instanceof TemporaryTableScanOperator))
            {
                inner = context.wrap(new CachingOperator(context.acquireNodeId(), inner));
            }
            // If we have a correlated query and a inner index
            // wrap inner operator with an outer values operator to
            // let the inner utilize each nested loops outer rows index values
            else if (hasIndex)
            {
                IOrdinalValuesFactory outerOrdinalValuesFactory = createOrdinalValuesFactory(context.session, foundation.outerValueExpressions);
                inner = context.wrap(new OuterValuesOperator(context.acquireNodeId(), inner, outerOrdinalValuesFactory));
            }

            return new NestedLoopJoin(context.acquireNodeId(), logicalOperator, outer, inner, predicate, new DefaultTupleMerger(-1, innerTableSource.getTableAlias()
                    .getTupleOrdinal(), compositeTupleCountOnLevel), populating, emitEmptyOuterRows);
        }

        // No indices for inner -> HashJoin
        if (indexPredicate == null
                || forceHashJoin(context.session))
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());
            // TODO: implement hash of inner and probe of outer
            // Option hashInner = getOption(innerTableSource, HASH_INNER);
            return new HashJoin(context.acquireNodeId(), logicalOperator, outer, inner, createHashFunction(context.session, foundation.outerValueExpressions),
                    createHashFunction(context.session, foundation.innerValueExpressions), predicate, new DefaultTupleMerger(-1, innerTableSource.getTableAlias()
                            .getTupleOrdinal(), compositeTupleCountOnLevel),
                    populating, emitEmptyOuterRows);
        }

        context.indexPredicate = indexPredicate;

        // Option cachefilter = getOption(innerTableSource, "cachefilter");
        // TODO: not cacheFilter then extract predicates before visiting inner table source

        innerTableSource.accept(this, context);
        if (context.indexPredicate != null)
        {
            throw new RuntimeException("Index " + indexPredicate.getIndex() + " should have been consumed by " + innerTableSource.getTable());
        }

        Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());

        constantalizeValueExtractors(foundation.outerValueExpressions, foundation.innerValueExpressions);
        IOrdinalValuesFactory outerOrdinalValuesFactory = createOrdinalValuesFactory(context.session, foundation.outerValueExpressions);
        ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction = createHashFunction(context.session, foundation.innerValueExpressions);

        Option cacheName = getOption(innerTableSource, "cachename");
        Option cacheKey = getOption(innerTableSource, "cachekey");
        Option cachettl = getOption(innerTableSource, "cachettl");
        Option parallelism = getOption(innerTableSource, "parallelism");

        if (cacheName != null
                && !batchCacheDisabled(context.session))
        {
            IOrdinalValuesFactory innerOrdinalValuesFactory = createOrdinalValuesFactory(context.session, foundation.innerValueExpressions);
            // When caching we need to have asterisk columns else is will be strange to select different columns
            // then the original query
            BatchCacheOperator.CacheSettings settings = new BatchCacheOperator.CacheSettings(new ExpressionFunction(cacheName.getValueExpression()),
                    cacheKey != null ? new ExpressionFunction(cacheKey.getValueExpression())
                            : null,
                    cachettl != null ? new ExpressionFunction(cachettl.getValueExpression())
                            : null);

            inner = context.wrap(new BatchCacheOperator(context.acquireNodeId(), inner, innerOrdinalValuesFactory, settings));
        }

        // Get batch size option for provided table source (if any) Used to override default {@link Index#getBatchSize()} for a table
        Option batchSizeOption = getOption(innerTableSource, BATCH_SIZE);

        int batchParallelismOuterNodeId;
        if (parallelism != null)
        {
            batchParallelismOuterNodeId = context.acquireNodeId();
            Operator outerOp = context.wrap(new BatchParallelismOperator.BatchhParallelismOuterOperator(batchParallelismOuterNodeId));
            return new BatchParallelismOperator(context.acquireNodeId(), outer,
                    context.wrap(new BatchHashJoin(context.acquireNodeId(), logicalOperator, outerOp, inner, outerOrdinalValuesFactory, innerHashFunction, predicate != null ? predicate
                            : ctx -> true,
                            new DefaultTupleMerger(-1, innerTableSource.getTableAlias()
                                    .getTupleOrdinal(), compositeTupleCountOnLevel),
                            populating, emitEmptyOuterRows, indexPredicate.getIndex(), batchSizeOption)),
                    batchParallelismOuterNodeId, indexPredicate.getIndex()
                            .getBatchSize());
        }
        else
        {
            return new BatchHashJoin(context.acquireNodeId(), logicalOperator, outer, inner, outerOrdinalValuesFactory, innerHashFunction, predicate != null ? predicate
                    : ctx -> true,
                    new DefaultTupleMerger(-1, innerTableSource.getTableAlias()
                            .getTupleOrdinal(), compositeTupleCountOnLevel),
                    populating, emitEmptyOuterRows, indexPredicate.getIndex(), batchSizeOption);
        }
    }

    private boolean forceHashJoin(QuerySession session)
    {
        return BooleanUtils.isTrue((Boolean) session.getSystemProperty("force.hash_join"));
    }

    private boolean batchCacheDisabled(QuerySession session)
    {
        return BooleanUtils.isTrue((Boolean) session.getSystemProperty(QuerySession.BATCH_CACHE_DISABLED));
    }

    private Projection createRootProjection(Context context, RootProjection projection)
    {
        if (context.codeGenerateRootProjection
                && BooleanUtils.isTrue((Boolean) context.session.getSystemProperty(QuerySession.CODEGEN_ENABLED)))
        {
            try
            {
                return CODE_GENERATOR.generateProjection(projection);
            }
            catch (Exception e)
            {
                String message = e.getMessage();
                e.printStackTrace(new PrintWriter(context.session.getPrintWriter()));
                context.session.printLine("Could not generate code for projection '" + projection + "' Fallback to evaluating. " + message);
            }
        }

        return projection;
    }

    private Predicate<ExecutionContext> createPredicate(QuerySession session, Expression predicate)
    {
        if (predicate == null)
        {
            return null;
        }

        if (BooleanUtils.isTrue((Boolean) session.getSystemProperty(QuerySession.CODEGEN_ENABLED))
                && predicate.isCodeGenSupported())
        {
            try
            {
                return CODE_GENERATOR.generatePredicate(predicate);
            }
            catch (Exception e)
            {
                String message = e.getMessage();
                e.printStackTrace(new PrintWriter(session.getPrintWriter()));
                session.printLine("Could not generate code for predicate: '" + predicate + "' Fallback to evaluating. " + message);
            }
        }

        return new ExpressionPredicate(predicate);
    }

    private IOrdinalValuesFactory createOrdinalValuesFactory(QuerySession session, List<Expression> expressions)
    {
        if (BooleanUtils.isTrue((Boolean) session.getSystemProperty(QuerySession.CODEGEN_ENABLED))
                && expressions.stream()
                        .allMatch(Expression::isCodeGenSupported))
        {
            try
            {
                return CODE_GENERATOR.generateOrdinalValuesFactory(expressions);
            }
            catch (Exception e)
            {
                String message = e.getMessage();
                e.printStackTrace(new PrintWriter(session.getPrintWriter()));
                session.printLine("Could not generate code for hash function: '" + expressions + "' Fallback to evaluating. " + message);
            }
        }

        return new ExpressionOrdinalValuesFactory(expressions);
    }

    private ToIntBiFunction<ExecutionContext, Tuple> createHashFunction(QuerySession session, List<Expression> expressions)
    {
        if (BooleanUtils.isTrue((Boolean) session.getSystemProperty(QuerySession.CODEGEN_ENABLED))
                && expressions.stream()
                        .allMatch(Expression::isCodeGenSupported))
        {
            try
            {
                return CODE_GENERATOR.generateHashFunction(expressions);
            }
            catch (Exception e)
            {
                String message = e.getMessage();
                e.printStackTrace(new PrintWriter(session.getPrintWriter()));
                session.printLine("Could not generate code for hash function: '" + expressions + "' Fallback to evaluating. " + message);
            }
        }

        return new ExpressionHashFunction(expressions);
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
            if (outerValueExpressions.get(i)
                    .isConstant())
            {
                innerValueExpressions.set(i, outerValueExpressions.get(i));
            }
            else if (innerValueExpressions.get(i)
                    .isConstant())
            {
                outerValueExpressions.set(i, innerValueExpressions.get(i));
            }
        }
    }

    /** Wraps provided operator with push down predicate filter for provided alias **/
    private Operator wrapWithPushDown(Context context, Operator operator, TableAlias tableAlias)
    {
        List<IAnalyzePair> pushDownPairs = context.pushDownPredicateByAlias.remove(tableAlias.getAlias());
        /* Apply any pushdown filter */
        if (pushDownPairs != null)
        {
            Expression pushDownPredicate = new AnalyzeResult(pushDownPairs).getPredicate();
            return context.wrap(new FilterOperator(context.acquireNodeId(), operator, createPredicate(context.session, pushDownPredicate)));
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

        Catalog catalog = context.session.getCatalogRegistry()
                .getCatalog(catalogAlias);
        if (catalog == null)
        {
            throw new ParseException("No catalog found with name " + catalogAlias, token);
        }

        return Pair.of(catalogAlias, catalog);
    }

    /** Create a dummy operator for selects without a FROM clause */
    private Operator getEmptyTableSourceOperator(Context context)
    {
        return new NoOpOperator(context.acquireNodeId());
    }

    /**
     * No op operator used for selects with no table source It returns ONE dummy NoOp tuple
     */
    private static class NoOpOperator extends AOperator
    {
        private static final NoOpTupleList TUPLE_LIST = new NoOpTupleList();

        private NoOpOperator(int nodeId)
        {
            super(nodeId);
        }

        @Override
        public String getName()
        {
            return "Empty operator";
        }

        @Override
        public TupleIterator open(IExecutionContext context)
        {
            return TUPLE_LIST;
        }

        /** No op iterator */
        private static class NoOpTupleList implements TupleList
        {
            @Override
            public int size()
            {
                return 1;
            }

            @Override
            public Tuple get(int index)
            {
                if (index == 0)
                {
                    return NoOpTuple.NO_OP;
                }
                throw new IndexOutOfBoundsException("" + index);
            }
        }
    }

    /**
     * Detects correlated queries by analyzing qualified expression references in child join to see if any aliases from outer context is referenced.
     */
    private static class CorrelatedDetector extends AStatementVisitor<Void, Set<Integer>>
    {
        private final AExpressionVisitor<Void, Set<Integer>> expressionVisitor = new AExpressionVisitor<Void, Set<Integer>>()
        {
            @Override
            public Void visit(QualifiedReferenceExpression expression, Set<Integer> tupleOrdinals)
            {
                // Collect all referenced target tuple ordinals
                for (ResolvePath path : expression.getResolvePaths())
                {
                    tupleOrdinals.add(path.getTargetTupleOrdinal());
                }
                return null;
            }
        };

        @Override
        protected void visitExpression(Set<Integer> context, Expression expression)
        {
            expression.accept(expressionVisitor, context);
        }

        boolean isCorrelated(AJoin join)
        {
            // It must be a function or a sub query to match a correlated sub query
            TableAlias current = join.getTableSource()
                    .getTableAlias();
            if (!(current.getType() == TableAlias.Type.SUBQUERY
                    || current.getType() == TableAlias.Type.FUNCTION))
            {
                return false;
            }

            // Collect all sibling tuple ordinals
            List<TableAlias> siblingAliases = current.getSiblingAliases();
            Set<Integer> outerOrdinals = new HashSet<>(siblingAliases.size());
            for (TableAlias alias : siblingAliases)
            {
                // The current alias is ok to reference without qualifying as a correlated
                if (alias != current)
                {
                    outerOrdinals.add(alias.getTupleOrdinal());
                }
            }

            // If any of these are referenced in provided join then we have a
            // correlated query
            Set<Integer> joinOrdinals = new HashSet<>();
            join.getTableSource()
                    .accept(this, joinOrdinals);

            return containsAny(outerOrdinals, joinOrdinals);
        }
    }
}
