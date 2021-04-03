package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.kuse.payloadbuilder.core.parser.LiteralIntegerExpression;
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

        Map<TableAlias, Set<String>> columnsByAlias = new LinkedHashMap<>();

        /** Resulting operator */
        private Operator operatorResult;

        /** Resulting projection */
        private Projection projection;

        /** Index if any that should be picked up when creating table operator */
        private Index index;

        /** Predicate pushed down from join to table source */
        private final Map<String, List<AnalyzePair>> pushDownPredicateByAlias = new THashMap<>();

        /** Sort items. Sent to catalog to for usage if supported. */
        private List<SortItem> sortItems = emptyList();

        /**
         * Analyze mode. If true a intercepting operator will be inserted in front of each operator that analyzes and performs measurement etc.
         */
        private boolean analyze;

        /** Acquire next unique node id */
        private int acquireNodeId()
        {
            return nodeId++;
        }

        /** Set current operator */
        private void setOperator(Operator operator)
        {
            if (analyze && !(operator instanceof AnalyzeOperator))
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
            if (analyze && !(operator instanceof AnalyzeOperator))
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
        return create(session, select, false);
    }

    /** Create operator */
    public static Pair<Operator, Projection> create(QuerySession session, Select select, boolean analyze)
    {
        Context context = new Context();
        context.analyze = analyze;
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

        return Pair.of(context.getOperator(), context.projection);
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

        // TODO: This thing might need some rewrite, it's a very loose coupling
        // between this selects order by and which eventual catalogs table source that will pick it
        context.sortItems = new ArrayList<>(select.getOrderBy());
        List<SortItem> prevItems = context.sortItems;

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
                    context.setOperator(new FilterOperator(context.acquireNodeId(), context.getOperator(), new ExpressionPredicate(predicate)));
                }
            }
        }

        if (batchLimitOption != null)
        {
            batchLimitId = context.acquireNodeId();
            context.setOperator(new BatchLimitOperator(batchLimitId, context.getOperator(), batchLimitOption.getValueExpression()));
        }

        for (int i = 0; i < joinSize; i++)
        {
            joins.get(i).accept(this, context);
        }

        context.sortItems = prevItems;
        Pair<List<SelectItem>, List<SortItem>> pair = pushDownComputedColumns(select, context);
        List<SelectItem> items = pair.getKey();
        List<SortItem> sortItems = pair.getValue();

        if (where != null)
        {
            context.setOperator(new FilterOperator(context.acquireNodeId(), context.getOperator(), new ExpressionPredicate(where)));
            visit(where, context);
        }

        List<Expression> groupBys = select.getGroupBy();
        if (!select.getGroupBy().isEmpty())
        {
            groupBys.forEach(e -> visit(e, context));
            context.setOperator(createGroupBy(context.acquireNodeId(), groupBys, context.getOperator()));
        }

        if (batchLimitOption != null)
        {
            context.setOperator(new BatchRepeatOperator(context.acquireNodeId(), batchLimitId, context.getOperator()));
        }

        if (!sortItems.isEmpty())
        {
            sortItems.forEach(si -> si.accept(this, context));
            context.setOperator(new OrderByOperator(context.acquireNodeId(), context.getOperator(), new ExpressionTupleComparator(sortItems)));
        }

        if (select.getTopExpression() != null)
        {
            visit(select.getTopExpression(), context);
            context.setOperator(new TopOperator(context.acquireNodeId(), context.getOperator(), select.getTopExpression()));
        }

        context.currentTableAlias = tsj != null ? tsj.getTableSource().getTableAlias() : null;

        // Before processing the select items set the collection alias property in context to mark
        // that this should be used
        context.currentTableAliases = context.currentTableAlias != null ? asSet(context.currentTableAlias) : emptySet();

        List<String> projectionAliases = new ArrayList<>();
        List<Projection> projections = new ArrayList<>();
        items.forEach(s ->
        {
            s.accept(this, context);
            projectionAliases.add(defaultIfBlank(s.getIdentifier(), ""));
            projections.add(context.projection);
        });

        context.projection = new ObjectProjection(projectionAliases, projections);

        context.currentTableAliases = null;

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
            fromOperator = new OrderByOperator(context.acquireNodeId(), fromOperator, new ExpressionTupleComparator(nestedSelectItem.getOrderBy()));
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
                    if (tempAlias == null)
                    {
                        tempAlias = alias.getSiblingAlias(selectItem.getAlias());
                    }
                    if (tempAlias != null)
                    //CSON
                    {
                        tempAlias.setAsteriskColumns();
                        tupleOrdinals.add(tempAlias.getTupleOrdinal());
                        continue;
                    }
                }
            }
            else
            {
                alias.setAsteriskColumns();
                List<TableAlias> siblingAliases = alias.getSiblingAliases();

                for (TableAlias siblingAliase : siblingAliases)
                {
                    siblingAliase.setAsteriskColumns();
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
    public Void visit(Table table, Context context)
    {
        context.currentTableAlias = table.getTableAlias();
        if (table.isTempTable())
        {
            context.setOperator(new TemporaryTableScanOperator(context.acquireNodeId(), table));
            return null;
        }

        Pair<String, Catalog> pair = getCatalog(context, table.getCatalogAlias(), table.getToken());
        int nodeId = context.acquireNodeId();
        String alias = table.getTableAlias().getAlias();
        List<AnalyzePair> predicatePairs = ObjectUtils.defaultIfNull(context.pushDownPredicateByAlias.get(alias), emptyList());
        if (context.index != null)
        {
            Operator catalogOperator = pair.getValue()
                    .getIndexOperator(new OperatorData(
                            context.session,
                            nodeId,
                            pair.getKey(),
                            table.getTableAlias(),
                            predicatePairs,
                            emptyList(),
                            table.getOptions()),
                            context.index);

            if (catalogOperator == null)
            {
                throw new OperatorException("No operator returned from catalog " + pair.getKey() + " for table " + table.getTable());
            }

            context.setOperator(catalogOperator);
            context.index = null;
        }
        else
        {
            Operator catalogOperator = pair.getValue()
                    .getScanOperator(new OperatorData(
                            context.session,
                            nodeId,
                            pair.getKey(),
                            table.getTableAlias(),
                            predicatePairs,
                            context.sortItems,
                            table.getOptions()));

            if (catalogOperator == null)
            {
                throw new OperatorException("No operator returned from catalog " + pair.getKey() + " for table " + table.getTable());
            }

            context.setOperator(catalogOperator);
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
        context.setOperator(new TableFunctionOperator(context.acquireNodeId(), tableFunction.getCatalogAlias(), tableFunction.getTableAlias(), functionInfo, tableFunction.getArguments()));
        return null;
    }

    /**
     * <pre>
     * Push downs all expressions select items that are referenced in order by
     * this to be able to sort by expression items that would otherwise be invisible to
     * the sort operator.
     *
     * Also pushes down ordinal referenced columns like "ORDER BY 1"
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
     *   Post Projection: _Expre01 col, newCol
     *   Post operator tree
     *     sort(newCol)
     *       computedColumns (newCol: col * 2, _Expr01: col)
     *         scan(table)
     * </pre>
     **/
    //CSOFF
    private Pair<List<SelectItem>, List<SortItem>> pushDownComputedColumns(Select select, Context context)
    //CSON
    {
        // No FROM or no sort or assignment select then drop out
        // cause
        if (context.getOperator() == null
            || context.sortItems.isEmpty()
            || select.getSelectItems().stream().anyMatch(i -> i.getAssignmentName() != null))
        {
            return Pair.of(select.getSelectItems(), context.sortItems);
        }

        // Index select items by non qualified expressions, these are the ones we want to push down

        Map<String, Integer> expressionItemsByIndex = new HashMap<>();
        int index = 0;
        for (SelectItem item : select.getSelectItems())
        {
            if (item instanceof ExpressionSelectItem
                && !(((ExpressionSelectItem) item).getExpression() instanceof QualifiedReferenceExpression))
            {
                expressionItemsByIndex.put(lowerCase(item.getIdentifier()), index);
            }
            index++;
        }

        List<SelectItem> selectItems = new ArrayList<>(select.getSelectItems());
        List<SortItem> sortItems = new ArrayList<>(context.sortItems);
        List<String> columns = null;
        List<Expression> computedExpressions = null;

        int size = context.sortItems.size();
        for (int i = 0; i < size; i++)
        {
            SortItem item = context.sortItems.get(i);
            Integer itemIndex = null;

            // Sort item that references an expression from select items
            if (item.getExpression() instanceof QualifiedReferenceExpression
                && ((QualifiedReferenceExpression) item.getExpression()).getQname().getParts().size() == 1)
            {
                String column = ((QualifiedReferenceExpression) item.getExpression()).getQname().getFirst();
                itemIndex = expressionItemsByIndex.get(lowerCase(column));
            }
            // Sort item that references an select item by ordinal
            else if (item.getExpression() instanceof LiteralIntegerExpression)
            {
                // 1-based
                itemIndex = ((LiteralIntegerExpression) item.getExpression()).getValue() - 1;
            }

            // Item does not need modifications
            if (itemIndex == null)
            {
                continue;
            }
            else if (itemIndex >= selectItems.size())
            {
                throw new ParseException("ORDER BY position is out of range", item.getToken());
            }

            if (computedExpressions == null)
            {
                computedExpressions = new ArrayList<>();
                columns = new ArrayList<>();
            }

            SelectItem selectItem = selectItems.get(itemIndex);

            if (!(selectItem instanceof ExpressionSelectItem))
            {
                throw new ParseException("ORDER BY position is not supported for asterisk selects", item.getToken());
            }

            Expression expression = ((ExpressionSelectItem) selectItem).getExpression();
            // Visit the expression since it won't be resolved otherwise because
            // we're replacing it with a pre-resolved expression that will be skipped
            // later on
            visit(expression, context);

            // Extract current expression index
            index = computedExpressions.size();
            columns.add(selectItem.getIdentifier());
            computedExpressions.add(expression);

            Expression newReference = new QualifiedReferenceExpression(
                    QualifiedName.of(selectItem.getIdentifier()),
                    -1,
                    asList(new QualifiedReferenceExpression.ResolvePath(-1, -1, emptyList(), index)),
                    null);

            // Replace SelectItem
            selectItems.set(itemIndex, new ExpressionSelectItem(newReference,
                    selectItem.getIdentifier(),
                    null,
                    selectItem.getToken()));

            // Replace SortItem
            sortItems.set(i, new SortItem(newReference, item.getOrder(), item.getNullOrder(), item.getToken()));
        }

        if (computedExpressions != null)
        {
            context.setOperator(new ComputedColumnsOperator(context.acquireNodeId(), context.getOperator(), columns, computedExpressions));
        }

        return Pair.of(selectItems, sortItems);
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
        Operator outer = context.getOperator();
        context.setOperator(null);

        boolean isCorrelated = correlatedDetector.isCorrelated(context, join);

        Operator joinOperator = createJoin(
                context,
                logicalOperator,
                outer,
                join.getTableSource(),
                condition,
                emitEmptyOuterRows,
                isCorrelated);
        context.setOperator(joinOperator);
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
        boolean innerIsTempTable = false;

        // Sub query, use the inner table sources table and catalog when resolving indices
        if (innerTableSource instanceof SubQueryTableSource)
        {
            // TODO: needs some refactoring when doing nested unions etc.
            SubQueryTableSource subQuery = (SubQueryTableSource) innerTableSource;
            catalogAlias = subQuery.getSelect().getFrom().getTableSource().getCatalogAlias();
            table = subQuery.getSelect().getFrom().getTableSource().getTable();
        }
        else if (innerTableSource instanceof Table)
        {
            innerIsTempTable = ((Table) innerTableSource).isTempTable();
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
            // TODO: needs some refactoring when doing nested unions etc.
            String alias = ((SubQueryTableSource) innerTableSource).getSelect().getFrom().getTableSource().getTableAlias().getAlias();

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

            Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());

            // Cache inner operator if this is a regular nested loop
            // and it's not a temp table.
            if (!isCorrelated && !innerIsTempTable)
            {
                inner = context.wrap(new CachingOperator(context.acquireNodeId(), inner));
            }
            // If we have a correlated query and a inner index
            // wrap inner operator with an outer values operator to
            // let the inner utilize each nested loops outer rows index values
            else if (hasIndex)
            {
                inner = context.wrap(new OuterValuesOperator(context.acquireNodeId(), inner, foundation.outerValueExpressions));
            }

            return new NestedLoopJoin(
                    context.acquireNodeId(),
                    logicalOperator,
                    outer,
                    inner,
                    condition != null ? new ExpressionPredicate(condition) : null,
                    new DefaultTupleMerger(-1, innerTableSource.getTableAlias().getTupleOrdinal()),
                    populating,
                    emitEmptyOuterRows);
        }

        // No indices for inner -> HashJoin
        if (index == null)
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());
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
                    new DefaultTupleMerger(-1, innerTableSource.getTableAlias().getTupleOrdinal()),
                    populating,
                    emitEmptyOuterRows);
        }

        context.index = index;
        innerTableSource.accept(this, context);
        if (context.index != null)
        {
            throw new RuntimeException("Index " + index + " should have been consumed by " + innerTableSource.getTable());
        }
        Operator inner = wrapWithPushDown(context, context.getOperator(), innerTableSource.getTableAlias());

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
                new DefaultTupleMerger(-1, innerTableSource.getTableAlias().getTupleOrdinal()),
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
            return context.wrap(new FilterOperator(context.acquireNodeId(), operator, new ExpressionPredicate(pushDownPredicate)));
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
