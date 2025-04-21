package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlanVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Join.Type;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableSource;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.ProjectionMerger;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeInterceptor;
import se.kuseman.payloadbuilder.core.physicalplan.CachePlan;
import se.kuseman.payloadbuilder.core.physicalplan.ExpressionPredicate;
import se.kuseman.payloadbuilder.core.physicalplan.HashAggregate;
import se.kuseman.payloadbuilder.core.physicalplan.HashMatch;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.physicalplan.NestedLoop;
import se.kuseman.payloadbuilder.core.planning.ConditionAnalyzer.Result;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner.Context;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner.TableSourcePushDown;

/** Class that transform a {@link ILogicalPlan} to a {@link IPhysicalPlan} */
class QueryPlanner implements ILogicalPlanVisitor<IPhysicalPlan, StatementPlanner.Context>
{
    static final QualifiedName FORCE_NO_INDEX = QualifiedName.of("forceNoIndex");

    @Override
    public IPhysicalPlan visit(Projection plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);

        List<IExpression> expressions = plan.getExpressions();
        if (input instanceof se.kuseman.payloadbuilder.core.physicalplan.Projection p)
        {
            expressions = ProjectionMerger.replace(plan.getExpressions(), p.getExpressions());
            input = p.getInput();
        }
        else if (input instanceof AnalyzeInterceptor ai
                && ai.getInput() instanceof se.kuseman.payloadbuilder.core.physicalplan.Projection p)
        {
            expressions = ProjectionMerger.replace(plan.getExpressions(), p.getExpressions());
            input = p.getInput();
        }

        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Projection(context.getNextNodeId(), input, expressions));
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

        // If the catalog did not consume all predicates we will have the non pushable predicates
        // on top and the pushable left overs as child, merge the predictes
        if (input instanceof se.kuseman.payloadbuilder.core.physicalplan.Filter)
        {
            se.kuseman.payloadbuilder.core.physicalplan.Filter filter = (se.kuseman.payloadbuilder.core.physicalplan.Filter) input;
            predicate = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, predicate, ((ExpressionPredicate) filter.getPredicate()).getPredicate());
            input = filter.getInput();
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

        // CSOFF
        boolean hasSortItems = !sortItems.isEmpty();
        // CSON

        Catalog catalog = plan.isTempTable() ? SystemCatalog.get()
                : context.getSession()
                        .getCatalog(plan.getTableSource()
                                .getCatalogAlias());

        SeekPredicate seekPredicate = null;
        IDatasource dataSource;

        /* @formatter:off
         * See if we have a predicate that matches an index we can use
         * ie.
         *
         * select *
         * from tableA a
         * where a.column in (1,2,3)  <-- if there is an index on 'column' we can utilize an index for this
         * @formatter:on
         */
        if (context.seekPredicate == null
                && predicatePairs != null)
        {
            context.seekPredicate = ConditionAnalyzer.getSeekPredicate(plan, predicatePairs);
        }

        int nodeId = context.getNextNodeId();

        final Optional<Schema> schema = SchemaUtils.isAsterisk(plan.getSchema()) ? Optional.empty()
                : Optional.of(plan.getSchema());

        if (plan.isTempTable())
        {
            QualifiedName tableName = plan.getTableSource()
                    .getName()
                    .toLowerCase();

            if (context.seekPredicate != null)
            {
                seekPredicate = context.seekPredicate;
                context.seekPredicate = null;
            }

            dataSource = new TemporaryTableDataSource(schema, tableName, seekPredicate);
        }
        else
        {
            int sortItemCount = sortItems.size();
            String catalogAlias = defaultIfBlank(plan.getTableSource()
                    .getCatalogAlias(),
                    context.getSession()
                            .getDefaultCatalogAlias());

            DatasourceData data = new DatasourceData(nodeId, schema, predicatePairs, sortItems, plan.getProjection(), plan.getOptions());

            if (context.seekPredicate != null)
            {
                seekPredicate = context.seekPredicate;
                context.seekPredicate = null;
                dataSource = catalog.getSeekDataSource(context.getSession(), catalogAlias, seekPredicate, data);
            }
            else
            {
                dataSource = catalog.getScanDataSource(context.getSession(), catalogAlias, plan.getTableSource()
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

        return wrapWithAnalyze(context, seekPredicate != null
                ? new se.kuseman.payloadbuilder.core.physicalplan.IndexSeek(nodeId, plan.getSchema(), plan.getTableSource(), catalog.getName(), plan.isTempTable(), seekPredicate, dataSource,
                        plan.getOptions())
                : new se.kuseman.payloadbuilder.core.physicalplan.TableScan(nodeId, plan.getSchema(), plan.getTableSource(), catalog.getName(), plan.isTempTable(), dataSource, plan.getOptions()));
    }

    @Override
    public IPhysicalPlan visit(TableFunctionScan plan, Context context)
    {
        Pair<String, TableFunctionInfo> pair = context.getSession()
                .resolveTableFunctionInfo(plan.getTableSource()
                        .getCatalogAlias(),
                        plan.getTableSource()
                                .getName()
                                .getFirst());

        String catalogAlias = pair.getKey();

        Catalog catalog = context.getSession()
                .getCatalog(catalogAlias);

        TableFunctionInfo functionInfo = pair.getValue();
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.TableFunctionScan(context.getNextNodeId(), plan.getSchema(), plan.getTableSource(), catalogAlias,
                catalog.getName(), functionInfo, plan.getArguments(), plan.getOptions()));
    }

    @Override
    public IPhysicalPlan visit(ExpressionScan plan, Context context)
    {
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ExpressionScan(context.getNextNodeId(), plan.getTableSource(), plan.getSchema(), plan.getExpression()));
    }

    @Override
    public IPhysicalPlan visit(SubQuery plan, Context context)
    {
        /* Eliminate the subquery, should not exist in the final result */
        return plan.getInput()
                .accept(this, context);
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
            throw new IllegalArgumentException("RIGHT joins are not supported");
        }

        IPhysicalPlan outer = plan.getOuter()
                .accept(this, context);

        IExpression condition = plan.getCondition();

        /* Seek predicate found. */
        SeekPredicate seekPredicate = null;

        /* Outer and inner equi expressions found. Used if a hash join should be used as physical join */
        List<IExpression> outerEquiExpressions = emptyList();
        List<IExpression> innerEquiExpressions = emptyList();

        if (condition != null)
        {
            List<TableSourceReference> tableSources;
            TableSource tableSource = getTopTableSource(plan.getInner(), false);
            // We need both the subquery TS and it's childs TS
            if (tableSource instanceof SubQuery s)
            {
                TableSource child = getTopTableSource(s.getInput(), true);
                tableSources = child != null ? List.of(tableSource.getTableSource(), child.getTableSource())
                        : List.of(tableSource.getTableSource());
            }
            else
            {
                tableSources = List.of(tableSource.getTableSource());
            }

            Result analyzeResult = ConditionAnalyzer.analyze(tableSources, condition, context.schemaByTableSource);

            boolean forceNoJoinIndex = false;
            if (tableSource instanceof TableScan ts)
            {
                forceNoJoinIndex = ts.getOptions()
                        .stream()
                        .filter(o -> FORCE_NO_INDEX.equalsIgnoreCase(o.getOption()))
                        .map(e -> e.getValueExpression()
                                .eval(context.context))
                        .map(v -> v.getPredicateBoolean(0))
                        .findAny()
                        .orElse(false);
            }
            if (!forceNoJoinIndex)
            {
                seekPredicate = analyzeResult.seekPredicate();
            }
            outerEquiExpressions = analyzeResult.outerValueExpressions();
            innerEquiExpressions = analyzeResult.innerValueExpressions();
        }
        // TODO: if we have a non correlated join with no condition (cross join/cross apply/outer apply)
        // then it's much better to switch the inputs so that we only query the inner plan ONCE
        // or might be, depends on :)
        // else if (plan.getOuterReferences()
        // .isEmpty())
        // {
        // System.out.println("Switch inputs of " + plan);
        // }

        boolean correlated = !plan.getOuterReferences()
                .isEmpty();
        boolean pushOuterReference = seekPredicate != null;
        context.seekPredicate = seekPredicate;

        IPhysicalPlan inner = plan.getInner()
                .accept(this, context);

        ExpressionPredicate predicate = plan.getCondition() != null ? new ExpressionPredicate(condition)
                : null;

        ValueVector forceNestedLoopProperty = context.context.getSession()
                .getSystemProperty(QuerySession.FORCE_NESTED_LOOP);
        boolean forceNestedLoop = !forceNestedLoopProperty.isNull(0)
                && forceNestedLoopProperty.getBoolean(0);

        IPhysicalPlan join = null;

        if (!correlated
                && !outerEquiExpressions.isEmpty()
                && !forceNestedLoop
                && !context.joinPreserveOuterOrder)
        {
            join = new HashMatch(context.getNextNodeId(), outer, inner, outerEquiExpressions, innerEquiExpressions, predicate, plan.getPopulateAlias(), plan.getType() == Type.LEFT,
                    seekPredicate != null);
        }
        else
        {
            ValueVector forceNoInnerCacheProperty = context.context.getSession()
                    .getSystemProperty(QuerySession.FORCE_NO_INNER_CACHE);
            boolean forceNoInnerCache = !forceNoInnerCacheProperty.isNull(0)
                    && forceNoInnerCacheProperty.getBoolean(0);

            // We can cache the inner plan if we have a non correlated plain nested loop
            if (!pushOuterReference
                    && !forceNoInnerCache
                    && !correlated
                    && !plan.isSwitchedInputs())
            {
                inner = wrapWithAnalyze(context, new CachePlan(context.getNextNodeId(), inner));
            }

            join = createNestedLoop(plan, context.getNextNodeId(), outer, inner, predicate, pushOuterReference);
        }

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
        if (ConstantScan.ONE_ROW_EMPTY_SCHEMA.equals(plan))
        {
            return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId(), TupleVector.CONSTANT));
        }
        else if (ConstantScan.ZERO_ROWS_EMPTY_SCHEMA.equals(plan))
        {
            return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId(), TupleVector.EMPTY));
        }

        // Zero row scan with a schema
        if (plan.getRowsExpressions()
                .isEmpty())
        {
            return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId(), TupleVector.of(plan.getSchema())));
        }

        // Strip all alias expression here since they are not needed anymore
        List<List<IExpression>> rowsExpressions = plan.getRowsExpressions()
                .stream()
                .map(list -> list.stream()
                        .map(e -> e instanceof AliasExpression ae ? ae.getExpression()
                                : e)
                        .toList())
                .toList();

        // If all the constant scans expressions are constant then we can calculate a result TupleVector
        // now instead of upon every execution
        if (rowsExpressions.stream()
                .allMatch(l -> l.stream()
                        .allMatch(IExpression::isConstant)))
        {
            TupleVector vector = se.kuseman.payloadbuilder.core.physicalplan.ConstantScan.vectorize(plan.getSchema(), rowsExpressions, context.context);
            return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId(), vector));
        }

        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.ConstantScan(context.getNextNodeId(), plan.getSchema(), rowsExpressions));
    }

    @Override
    public IPhysicalPlan visit(Limit plan, Context context)
    {
        IPhysicalPlan input = plan.getInput()
                .accept(this, context);
        return wrapWithAnalyze(context, new se.kuseman.payloadbuilder.core.physicalplan.Limit(context.getNextNodeId(), input, plan.getLimitExpression()));
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
        return new se.kuseman.payloadbuilder.core.physicalplan.Concatenation(context.getNextNodeId(), plan.getSchema(), inputs);
    }

    private IPhysicalPlan createNestedLoop(Join plan, int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            boolean pushOuterReference)
    {
        if (plan.getType() == Type.INNER
                || plan.getType() == Type.CROSS)
        {
            // CROSS JOIN / CROSS APPLY
            if (condition == null)
            {
                return plan.getOuterReferences()
                        .isEmpty() ? NestedLoop.innerJoin(nodeId, outer, inner, plan.getPopulateAlias(), plan.isSwitchedInputs()) // Non correlated
                                : NestedLoop.innerJoin(nodeId, outer, inner, plan.getOuterReferences(), plan.getPopulateAlias(), plan.getOuterSchema()); // Correlated
            }

            // INNER JOIN
            return plan.getOuterReferences()
                    .isEmpty() ? NestedLoop.innerJoin(nodeId, outer, inner, condition, plan.getPopulateAlias(), pushOuterReference)
                            : NestedLoop.innerJoin(nodeId, outer, inner, plan.getOuterReferences(), condition, plan.getPopulateAlias(), false);
        }
        else if (plan.getType() == Type.LEFT)
        {
            // OUTER APPLY
            if (condition == null)
            {
                return plan.getOuterReferences()
                        .isEmpty() ? NestedLoop.leftJoin(nodeId, outer, inner, plan.getPopulateAlias(), plan.isSwitchedInputs()) // Non correlated
                                : NestedLoop.leftJoin(nodeId, outer, inner, plan.getOuterReferences(), plan.getPopulateAlias(), plan.getOuterSchema()); // Correlated
            }
            // LEFT JOIN
            return plan.getOuterReferences()
                    .isEmpty() ? NestedLoop.leftJoin(nodeId, outer, inner, condition, plan.getPopulateAlias(), pushOuterReference)
                            : NestedLoop.leftJoin(nodeId, outer, inner, plan.getOuterReferences(), condition, plan.getPopulateAlias(), false);
        }

        throw new IllegalArgumentException("Cannot create a nested loop from: " + plan);
    }

    /** Returns the first occurrence of a table source from provided plan. */
    private TableSource getTopTableSource(ILogicalPlan plan, boolean skipSubQuery)
    {
        List<ILogicalPlan> queue = new ArrayList<>();
        queue.add(plan);

        while (queue.size() > 0)
        {
            ILogicalPlan current = queue.remove(0);

            if (current instanceof TableSource tableSource)
            {
                if (!skipSubQuery
                        || !(tableSource instanceof SubQuery))
                {
                    return tableSource;
                }
            }

            List<ILogicalPlan> children = current.getChildren();

            if (children.size() == 1)
            {
                queue.addAll(children);
            }
            else
            {
                if (current instanceof Join join)
                {
                    // We use the outer table source of joins to check for a table source
                    // Should be changed later on when a more complex analyze is made with join ordering etc.
                    queue.add(join.getOuter());
                }
            }
        }

        return null;
    }

    static IPhysicalPlan wrapWithAnalyze(Context context, IPhysicalPlan plan)
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
            ColumnReference cr = ((ColumnExpression) expression).getColumnReference();
            TableSourceReference tableRef = cr != null ? cr.tableSourceReference()
                    : null;
            if (tableRef != null)
            {
                context.add(tableRef);
            }
            return null;
        }
    }

    /** Datasource used for temporary tables */
    static class TemporaryTableDataSource implements IDatasource
    {
        private final Optional<Schema> plannedSchema;
        private final QualifiedName name;
        private final ISeekPredicate seekPredicate;

        TemporaryTableDataSource(Optional<Schema> plannedSchema, QualifiedName name, ISeekPredicate seekPredicate)
        {
            this.plannedSchema = plannedSchema;
            this.name = name;
            this.seekPredicate = seekPredicate;
        }

        @Override
        public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
        {
            TemporaryTable temporaryTable = ((ExecutionContext) context).getSession()
                    .getTemporaryTable(name);
            TupleVector vector = temporaryTable.getTupleVector();

            Schema vectorSchema = vector.getSchema();
            final Schema schema = plannedSchema.orElse(vectorSchema);

            if (seekPredicate != null)
            {
                final TupleIterator indexIterator = temporaryTable.getIndexIterator(context, seekPredicate);
                return new TupleIterator()
                {
                    @Override
                    public int estimatedBatchCount()
                    {
                        return indexIterator.estimatedBatchCount();
                    }

                    @Override
                    public int estimatedRowCount()
                    {
                        return indexIterator.estimatedRowCount();
                    }

                    @Override
                    public TupleVector next()
                    {
                        return wrap(schema, indexIterator.next());
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return indexIterator.hasNext();
                    }
                };
            }

            // Return the tuple vector from context with the planned schema
            return TupleIterator.singleton(wrap(schema, vector));
        }

        private TupleVector wrap(Schema schema, TupleVector temporaryTableVector)
        {
            final Schema vectorSchema = temporaryTableVector.getSchema();
            return new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    return schema;
                }

                @Override
                public int getRowCount()
                {
                    return temporaryTableVector.getRowCount();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    if (column >= vectorSchema.getSize())
                    {
                        return ValueVector.literalNull(plannedSchema.get()
                                .getColumns()
                                .get(column)
                                .getType(), temporaryTableVector.getRowCount());
                    }
                    return temporaryTableVector.getColumn(column);
                }
            };
        }

        @Override
        public int hashCode()
        {
            return plannedSchema.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this)
            {
                return true;
            }
            else if (obj == null)
            {
                return false;
            }
            else if (obj instanceof TemporaryTableDataSource)
            {
                TemporaryTableDataSource that = (TemporaryTableDataSource) obj;
                return plannedSchema.equals(that.plannedSchema)
                        && name.equals(that.name);
            }
            return false;
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

        AnalyzePair getAnalyzePair()
        {
            return analyzePair;
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
                    // Pick the first non null argument
                    IFunctionCallExpression functionCallExpression = getFunctionCallExpression();
                    for (IExpression arg : functionCallExpression.getArguments())
                    {
                        QualifiedName qualifiedColumn = arg.getQualifiedColumn();
                        if (qualifiedColumn != null)
                        {
                            return qualifiedColumn;
                        }
                    }
                    return null;
                case IN:
                    IInExpression inExpression = getInExpression();
                    return inExpression.getExpression()
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
        public IExpression getFullExpression()
        {
            return analyzePair.getPredicate();
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
