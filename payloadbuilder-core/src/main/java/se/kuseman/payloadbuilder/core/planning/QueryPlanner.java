package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlanVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Join.Type;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.OverScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeInterceptor;
import se.kuseman.payloadbuilder.core.physicalplan.ExpressionPredicate;
import se.kuseman.payloadbuilder.core.physicalplan.HashAggregate;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.physicalplan.NestedLoop;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner.Context;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner.TableSourcePushDown;

/** Class that transform a {@link ILogicalPlan} to a {@link IPhysicalPlan} */
class QueryPlanner implements ILogicalPlanVisitor<IPhysicalPlan, StatementPlanner.Context>
{
    @Override
    public IPhysicalPlan visit(Projection plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);

        // TODO: if the input plan is static that schema should be provided to the physical projection to
        // avoid recreating the schema on each execution
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Projection(context.getNextNodeId(), input, plan.getExpressions(), plan.isAppendInputColumns()));
    }

    @Override
    public IPhysicalPlan visit(Sort plan, Context context)
    {
        /*
         * @formatter:off
         * 
         * select *
         * from tableA a
         * inner join tableB b
         *  on .....
         * order by a.col
         * 
         * Order by can be pushed down only if referencing columns are from the FROM table source
         * If the order by is pushed down all joins on the level need to have preserve outer order
         * to make sure the output order is not altered since the sort operator will be removed
         * 
         * @formatter:on
         */

        Set<TableSourceReference> tableSources = new HashSet<>();
        plan.getSortItems()
                .forEach(i -> i.getExpression()
                        .accept(TableSourceReferenceCollector.INSTANCE, tableSources));

        StatementPlanner.TableSourcePushDown tableSourcePushDown = null;
        if (tableSources.size() == 1)
        {
            tableSourcePushDown = context.tableSourcePushDown.computeIfAbsent(tableSources.iterator()
                    .next(), k -> new StatementPlanner.TableSourcePushDown());
            tableSourcePushDown.sortItems = plan.getSortItems();
        }

        IPhysicalPlan input = plan.getInput()
                .accept(this, context);

        // Reset flag before we leave sort
        context.joinPreserveOuterOrder = false;

        // Sort items consumed no need to create a sort plan
        if (tableSourcePushDown != null
                && tableSourcePushDown.sortItems.isEmpty())
        {
            return input;
        }

        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Sort(context.getNextNodeId(), input, plan.getSortItems()));
    }

    @Override
    public IPhysicalPlan visit(Filter plan, Context context)
    {
        StatementPlanner.TableSourcePushDown tableSourcePushDown = null;
        if (plan.getTableSource() != null)
        {
            tableSourcePushDown = context.tableSourcePushDown.computeIfAbsent(plan.getTableSource(), k -> new StatementPlanner.TableSourcePushDown());
            tableSourcePushDown.predicatePairs = PredicateAnalyzer.analyze(plan.getPredicate())
                    .getPairs();
        }

        IPhysicalPlan input = plan.getInput()
                .accept(this, context);

        IExpression predicate = plan.getPredicate();
        if (tableSourcePushDown != null)
        {
            // All predicate pairs was consumed skip filter and return input
            if (tableSourcePushDown.predicatePairs.isEmpty())
            {
                return input;
            }

            // Construct a new predicate from the pairs that was not consumed
            predicate = AnalyzeResult.getPredicate(tableSourcePushDown.predicatePairs);
        }

        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Filter(context.getNextNodeId(), input, new ExpressionPredicate(predicate)));
    }

    @Override
    public IPhysicalPlan visit(Aggregate plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);
        return wrapWithAnalyze(context, new HashAggregate(context.getNextNodeId(), input, plan.getAggregateExpressions(), plan.getProjectionExpressions()));
    }

    @Override
    public IPhysicalPlan visit(TableScan plan, Context context)
    {
        TableSourcePushDown tableSourcePushDown = context.tableSourcePushDown.get(plan.getTableSource());

        List<? extends ISortItem> sortItems = emptyList();
        List<IPredicate> predicatePairs = emptyList();
        if (tableSourcePushDown != null)
        {
            sortItems = tableSourcePushDown.sortItems;
            predicatePairs = tableSourcePushDown.predicatePairs.stream()
                    .map(p -> new PredicatePair(p, plan.getTableSource()))
                    .collect(toList());
        }

        // Push down of sort items are only possible on the top (FROM)
        // table source
        if (context.topTableScanVisited)
        {
            sortItems = emptyList();
        }

        boolean hasSortItems = !sortItems.isEmpty();

        Catalog catalog = context.getSession()
                .getCatalog(plan.getCatalogAlias());

        SeekPredicate seekPredicate = null;
        IDatasource dataSource;

        int nodeId = context.getNextNodeId();

        if (plan.isTempTable())
        {
            dataSource = new IDatasource()
            {
                @Override
                public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
                {
                    TupleVector vector = ((ExecutionContext) context).getSession()
                            .getTemporaryTable(plan.getTableSource()
                                    .getName()
                                    .toDotDelimited()
                                    .toLowerCase());
                    return TupleIterator.singleton(vector);
                }
            };
        }
        else
        {
            int sortItemCount = sortItems.size();
            DatasourceData data = new DatasourceData(nodeId, predicatePairs, sortItems, plan.getProjection());

            if (context.seekPredicate != null)
            {
                seekPredicate = context.seekPredicate;
                dataSource = catalog.getSeekDataSource(context.getSession(), plan.getCatalogAlias(), context.seekPredicate, data);
                context.seekPredicate = null;
            }
            else
            {
                dataSource = catalog.getScanDataSource(context.getSession(), plan.getCatalogAlias(), plan.getTableSource()
                        .getName(), data);
            }

            // Validate sort items
            if (sortItemCount > 0
                    && sortItems.size() != sortItemCount
                    && !sortItems.isEmpty())
            {
                throw new CompileException("Sort items must be totally consumed or left as is.");
            }
        }

        // Set the left overs to context to be consumed when coming back to filter above
        if (tableSourcePushDown != null)
        {
            tableSourcePushDown.predicatePairs = predicatePairs.stream()
                    .map(p -> ((PredicatePair) p).analyzePair)
                    .collect(toList());
        }
        // If sort items was consumed by the catalog then mark that all joins up back to sort operator need to preserve it's outer
        // order since the sort operator will be removed
        if (hasSortItems
                && sortItems.isEmpty())
        {
            context.joinPreserveOuterOrder = true;
        }

        context.topTableScanVisited = true;

        return wrapWithAnalyze(context,
                seekPredicate != null ? new se.kuseman.payloadbuilder.core.physicalplan.IndexSeek(nodeId, plan.getSchema(), plan.getTableSource(), seekPredicate, dataSource, plan.getOptions())
                        : new se.kuseman.payloadbuilder.core.physicalplan.TableScan(nodeId, plan.getSchema(), plan.getTableSource(), plan.isTempTable(), dataSource, plan.getOptions()));
    }

    @Override
    public IPhysicalPlan visit(TableFunctionScan plan, Context context)
    {
        Pair<String, TableFunctionInfo> pair = context.getSession()
                .resolveTableFunctionInfo(plan.getCatalogAlias(), plan.getTableSource()
                        .getName()
                        .getFirst());

        String catalogAlias = pair.getKey();
        TableFunctionInfo functionInfo = pair.getValue();
        return wrapWithAnalyze(context,
                new se.kuseman.payloadbuilder.core.physicalplan.TableFunctionScan(context.getNextNodeId(), plan.getTableSource(), catalogAlias, functionInfo, plan.getArguments(), plan.getOptions()));
    }

    @Override
    public IPhysicalPlan visit(SubQuery plan, Context context)
    {
        throw new IllegalArgumentException("Sub queries cannot be planned");
    }

    @Override
    public IPhysicalPlan visit(Join plan, Context context)
    {
        // Flatten the join hierarchy to a list to be able to perform join ordering
        // and index selections etc.
        /* @formatter:off
         * 
         * 
         * 
         * select *
         * from tableA a
         * inner join tableB b
         *   on ...
         * inner join tableC c
         *   on ...
         * inner join tableD d
         *   on ...
         * 
         * Going from this:
         * 
         *   Join
         *     Join
         *       Join
         *         Scan: tableA
         *         Scan: tableB
         *       Scan: tableC
         *     Scan: tableD
         * 
         * To this:
         * 
         *   Outer: scan: tableA
         *   Joins: 
         *     - tableB
         *       condition:
         *       type: INNER
         *     - tableC
         *       condition:
         *       type: INNER
         *     - tableD
         *       condition:
         *       type: INNER
         * 
         * @formatter:on
         */
        // TODO: Join ordering
        // List<JoinedTableSource> joins = new ArrayList<>();
        // joins.add(new JoinedTableSource(plan, plan.getInner()));
        //
        // ILogicalPlan current = plan.getOuter();
        // while (current instanceof Join)
        // {
        // Join jc = (Join) current;
        // // Insert to position 0 to restore the original query order of joins
        // joins.add(0, new JoinedTableSource(jc, jc.getInner()));
        // current = ((Join) current).getOuter();
        // }
        //
        // // Current is the FROM table source
        // ILogicalPlan outer = current;
        //
        // if (joins.size() > 1)
        // {
        // System.out.println("Join ordering");
        // joins.stream()
        // .forEach(j -> System.out.println(j.inner.print(0)));
        // }

        if (plan.getType() == Type.RIGHT)
        {
            throw new IllegalArgumentException("RIGHT joins are not supported yet");
        }

        IPhysicalPlan outer = plan.getOuter()
                .accept(this, context);

        IExpression condition = plan.getCondition();

        /* Seek predicate found. */
        SeekPredicate seekPredicate = null;

        /* Outer and inner equi expressions found. Used if a hash join should be used as physical join */
        // List<IExpression> outerEquiExpressions = emptyList();
        // List<IExpression> innerEquiExpressions = emptyList();

        if (condition != null)
        {
            // For now we only use the available indices on the first following table scan
            // in the inner input. In the future this needs to be changed to a more dynamic
            // form by analyzing the predicate and what table source we are actual joining on
            // and apply join ordering etc.
            TableScan innerTableScan = getTopTableScan(plan.getInner());
            if (innerTableScan != null)
            {
                List<Index> indices = innerTableScan.getTableSchema()
                        .getIndices();

                IndexFoundation indexFoundation = new IndexFoundation(innerTableScan.getTableSource(), indices, condition);
                seekPredicate = indexFoundation.seekPredicate;
                // outerEquiExpressions = indexFoundation.outerValueExpressions;
                // innerEquiExpressions = indexFoundation.innerValueExpressions;
            }
        }
        // TODO: if we have a non correlated join with no condition (cross join/cross apply/outer apply)
        // then it's much better to switch the inputs so that we only query the inner plan ONCE
        // or might be, depends on :)
        // else if (plan.getOuterReferences()
        // .isEmpty())
        // {
        // System.out.println("Switch inputs of " + plan);
        // }

        boolean pushOuterReference = seekPredicate != null;
        context.seekPredicate = seekPredicate;

        IPhysicalPlan inner = plan.getInner()
                .accept(this, context);

        // boolean preserveOuterOrder = context.joinPreserveOuterOrder;

        ExpressionPredicate predicate = plan.getCondition() != null ? new ExpressionPredicate(condition)
                : null;

        // TODO: hash join etc.
        // preserveOuterOrder => force nested loop
        // outerEquiExpressions not empty => hash join
        IPhysicalPlan join = createNestedLoop(plan, context.getNextNodeId(), outer, inner, predicate, pushOuterReference);

        return wrapWithAnalyze(context, join);
    }

    @Override
    public IPhysicalPlan visit(OperatorFunctionScan plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);

        Pair<String, OperatorFunctionInfo> pair = context.getSession()
                .resolveOperatorFunctionInfo(plan.getCatalogAlias(), plan.getFunction());

        String catalogAlias = pair.getKey();
        OperatorFunctionInfo functionInfo = pair.getValue();
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.OperatorFunctionScan(context.getNextNodeId(), input, functionInfo, catalogAlias, plan.getSchema()));
    }

    @Override
    public IPhysicalPlan visit(ConstantScan plan, Context context)
    {
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId()));
    }

    @Override
    public IPhysicalPlan visit(Limit plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Limit(context.getNextNodeId(), input, plan.getLimitExpression()));
    }

    @Override
    public IPhysicalPlan visit(OverScan plan, Context context)
    {
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.OverScan(context.getNextNodeId(), plan.getOrdinal(), plan.getOverAlias(), plan.getSchema()));
    }

    @Override
    public IPhysicalPlan visit(MaxRowCountAssert plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);
        return wrapWithAnalyze(context, se.kuseman.payloadbuilder.core.physicalplan.Assert.maxRowCount(context.getNextNodeId(), input, plan.getMaxRowCount()));
    }

    @Override
    public IPhysicalPlan visit(Concatenation plan, Context context)
    {
        List<IPhysicalPlan> inputs = plan.getChildren()
                .stream()
                .map(c -> c.accept(this, context))
                .collect(toList());
        return new se.kuseman.payloadbuilder.core.physicalplan.Concatenation(context.getNextNodeId(), inputs);
    }

    private IPhysicalPlan createNestedLoop(Join plan, int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            boolean pushOuterReference)
    {
        if (plan.getType() == Type.INNER)
        {
            // CROSS JOIN / CROSS APPLY
            if (condition == null)
            {
                return plan.getOuterReferences()
                        .isEmpty() ? NestedLoop.innerJoin(nodeId, outer, inner, plan.getPopulateAlias(), plan.isSwitchedInputs()) // Non correlated
                                : NestedLoop.innerJoin(nodeId, outer, inner, plan.getOuterReferences(), plan.getPopulateAlias()); // Correlated
            }
            // INNER JOIN
            return NestedLoop.innerJoin(nodeId, outer, inner, condition, plan.getPopulateAlias(), pushOuterReference);
        }
        else if (plan.getType() == Type.LEFT)
        {
            // OUTER APPLY
            if (condition == null)
            {
                return plan.getOuterReferences()
                        .isEmpty() ? NestedLoop.leftJoin(nodeId, outer, inner, plan.getPopulateAlias()) // Non correlated
                                : NestedLoop.leftJoin(nodeId, outer, inner, plan.getOuterReferences(), plan.getPopulateAlias()); // Correlated
            }
            // LEFT JOIN
            return NestedLoop.leftJoin(nodeId, outer, inner, condition, plan.getPopulateAlias(), pushOuterReference);
        }

        throw new IllegalArgumentException("Cannot create a nested loop from: " + plan);
    }

    /** Returns the first occurrence of a table scan from provided plan. */
    private TableScan getTopTableScan(ILogicalPlan plan)
    {
        List<ILogicalPlan> queue = new ArrayList<>();
        queue.add(plan);

        while (queue.size() > 0)
        {
            ILogicalPlan current = queue.remove(0);

            if (current instanceof TableScan)
            {
                return (TableScan) current;
            }

            List<ILogicalPlan> children = current.getChildren();

            if (children.size() == 1)
            {
                queue.addAll(children);
            }
            else
            {
                if (current instanceof Join)
                {
                    // We use the outer table source of joins to check for a table source
                    // Should be changed later on when a more complex analyze is made with join ordering etc.
                    queue.add(((Join) current).getOuter());
                }
            }
        }

        return null;
    }

    private IPhysicalPlan wrapWithAnalyze(Context context, IPhysicalPlan plan)
    {
        if (context.analyze)
        {
            return new AnalyzeInterceptor(context.getNextNodeId(), plan);
        }
        return plan;
    }

    /** Visitor that collects table source references */
    static class TableSourceReferenceCollector extends AExpressionVisitor<Void, Set<TableSourceReference>>
    {
        private static final TableSourceReferenceCollector INSTANCE = new TableSourceReferenceCollector();

        @Override
        public Void visit(IColumnExpression expression, Set<TableSourceReference> context)
        {
            ColumnReference colRef = expression.getColumnReference();
            if (colRef != null)
            {
                context.add(colRef.getTableSource());
            }
            return null;
        }
    }

    /** Implementation of a {@link IPredicate} used by catalogs when pushing down predicates for a table source */
    static class PredicatePair implements IPredicate
    {
        private final AnalyzePair analyzePair;
        private final TableSourceReference tableSource;

        PredicatePair(AnalyzePair analyzePair, TableSourceReference tableSource)
        {
            this.analyzePair = analyzePair;
            this.tableSource = tableSource;
        }

        @Override
        public String getSqlRepresentation()
        {
            return analyzePair.getSqlRepresentation();
        }

        @Override
        public Type getType()
        {
            return analyzePair.getType();
        }

        @Override
        public QualifiedName getQualifiedColumn()
        {
            switch (analyzePair.getType())
            {
                case COMPARISION:
                    // The column is always located on the left side
                    return analyzePair.getExpressionPair(tableSource)
                            .getLeft()
                            .getQualifiedColumn();
                case FUNCTION_CALL:
                    return null;
                case IN:
                    return getInExpression().getExpression()
                            .getQualifiedColumn();
                case LIKE:
                    return getLikeExpression().getExpression()
                            .getQualifiedColumn();
                case NULL:
                    return getNullPredicateExpression().getExpression()
                            .getQualifiedColumn();
                case UNDEFINED:
                    return getUndefinedExpression().getQualifiedColumn();
                default:
                    return null;
            }
        }

        @Override
        public IExpression getComparisonExpression()
        {
            assertType(IPredicate.Type.COMPARISION);
            return analyzePair.getExpressionPair(tableSource)
                    .getRight();
        }

        @Override
        public IComparisonExpression.Type getComparisonType()
        {
            return analyzePair.getComparisonType();
        }

        @Override
        public IInExpression getInExpression()
        {
            assertType(IPredicate.Type.IN);
            return (IInExpression) analyzePair.getExpressionPair(tableSource)
                    .getRight();
        }

        @Override
        public ILikeExpression getLikeExpression()
        {
            assertType(IPredicate.Type.LIKE);
            return (ILikeExpression) analyzePair.getExpressionPair(tableSource)
                    .getRight();
        }

        @Override
        public INullPredicateExpression getNullPredicateExpression()
        {
            assertType(IPredicate.Type.NULL);
            return (INullPredicateExpression) analyzePair.getExpressionPair(tableSource)
                    .getRight();
        }

        @Override
        public IFunctionCallExpression getFunctionCallExpression()
        {
            assertType(IPredicate.Type.FUNCTION_CALL);
            return (IFunctionCallExpression) analyzePair.getExpressionPair(tableSource)
                    .getLeft();
        }

        @Override
        public IExpression getUndefinedExpression()
        {
            assertType(IPredicate.Type.UNDEFINED);
            return analyzePair.getExpressionPair(tableSource)
                    .getLeft();
        }

        private void assertType(IPredicate.Type type)
        {
            if (getType() != type)
            {
                throw new IllegalArgumentException("Pair is not of type " + type);
            }
        }
    }
}
