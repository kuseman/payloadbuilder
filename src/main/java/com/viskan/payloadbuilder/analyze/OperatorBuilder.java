package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.analyze.PredicateAnalyzer.AnalyzeItem;
import com.viskan.payloadbuilder.analyze.PredicateAnalyzer.AnalyzeResult;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.ArrayProjection;
import com.viskan.payloadbuilder.operator.BatchOperator;
import com.viskan.payloadbuilder.operator.BatchOperator.Reader;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionHashFunction;
import com.viskan.payloadbuilder.operator.ExpressionOperator;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.FilterOperator;
import com.viskan.payloadbuilder.operator.HashJoin;
import com.viskan.payloadbuilder.operator.NestedLoopJoin;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.TableFunctionOperator;
import com.viskan.payloadbuilder.parser.tree.AJoin;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Apply;
import com.viskan.payloadbuilder.parser.tree.Apply.ApplyType;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.ExpressionSelectItem;
import com.viskan.payloadbuilder.parser.tree.Join;
import com.viskan.payloadbuilder.parser.tree.Join.JoinType;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem.Type;
import com.viskan.payloadbuilder.parser.tree.PopulateTableSource;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.SelectItem;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableFunction;
import com.viskan.payloadbuilder.parser.tree.TableSource;
import com.viskan.payloadbuilder.parser.tree.TableSourceJoined;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;

import gnu.trove.map.hash.THashMap;

/**
 * Builder that constructs a {@link Operator} and {@link Projection} for a {@link Query}
 */
public class OperatorBuilder extends ATreeVisitor<Void, OperatorBuilder.Context>
{
    private static final OperatorBuilder VISITOR = new OperatorBuilder();

    private static final BiFunction<String, TableAlias, RuntimeException> MULTIPLE_ALIAS_EXCEPTION = (alias,
            parent) -> new IllegalArgumentException("Alias " + alias + " is defined multiple times for parent: " + parent);

    /** Context used during visiting tree */
    static class Context
    {
        private CatalogRegistry catalogRegistry;
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

        /** Table index to use for next table operator creation */
        private Index tableIndex;

        /** Predicate pushed down from join to table source */
        private final Map<String, Expression> pushDownPredicateByAlias = new THashMap<>();

        /** Appends push down predicate for provided alias */
        private void appendPushDownPredicate(String alias, Expression expression)
        {
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
    public static Pair<Operator, Projection> create(CatalogRegistry catalogRegistry, Query query)
    {
        Context context = new Context();
        context.catalogRegistry = catalogRegistry;
        query.accept(VISITOR, context);
        context.columnsByAlias.entrySet().forEach(e ->
        {
            if (e.getKey().getColumns() == null)
            {
                e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY));
            }
        });
        return Pair.of(context.operator, context.projection);
    }

    @Override
    public Void visit(Query query, Context context)
    {
        TableSourceJoined tsj = query.getFrom();

        Expression where = query.getWhere();
        Expression pushDownPredicate = null;
        if (where != null && tsj.getJoins().size() > 0)
        {
            /* TODO:
              Push down can applied to non populating joins ie:
              select s.id
              from source s
              inner join article a
                on a.id = s.id
              inner join brand b
                on b.id = a.bid
              where s.id > 0        <--- Pushed down to source
              and a.id2 < 10        <--- Can be pushed to article
              and b.id3 != 10       <--- can be pushed to brand
            
            */
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(where);
            pushDownPredicate = analyzeResult.extractPushdownPredicate(tsj.getTableSource().getAlias(), true);
            if (pushDownPredicate != null)
            {
                where = analyzeResult.getPredicate();
            }
        }

        // TODO: Utilize index of "from" clause if applicable
        //       ie. s.id = 10, s.id in (1,2,3,4,5)
        tsj.getTableSource().accept(this, context);

        // Collect columns from pushed down predicate
        if (pushDownPredicate != null)
        {
            context.operator = new FilterOperator(context.operator, new ExpressionPredicate(pushDownPredicate));
            visit(pushDownPredicate, context);
        }

        List<AJoin> joins = tsj.getJoins();
        int size = joins.size();
        for (int i = 0; i < size; i++)
        {
            joins.get(i).accept(this, context);
        }

        if (where != null)
        {
            context.operator = new FilterOperator(context.operator, new ExpressionPredicate(where));
            visit(where, context);
        }

        // TODO: wrap context.operator with group by
        query.getGroupBy().forEach(e -> visit(e, context));
        // TODO: wrap context.operator with order by
        query.getOrderBy().forEach(si -> si.accept(this, context));

        Map<String, Projection> rootProjections = new LinkedHashMap<>();

        query.getSelectItems().forEach(s ->
        {
            s.accept(this, context);
            rootProjections.put(s.getIdentifier(), context.projection);
        });

        context.projection = new ObjectProjection(rootProjections);
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
            fromOperator = new ExpressionOperator(from);
            aliases = ColumnsVisitor.getColumnsByAlias(context.columnsByAlias, context.parent, from);
        }

        Map<String, Projection> projections = new LinkedHashMap<>();
        boolean projectionsAdded = false;

        // Use found aliases and traverse select items, order and where
        TableAlias parent = context.parent;
        for (TableAlias alias : aliases)
        {
            context.parent = alias;
            for (SelectItem s : nestedSelectItem.getSelectItems())
            {
                s.accept(this, context);
                if (!projectionsAdded)
                {
                    projections.put(s.getIdentifier(), context.projection);
                }
            }

            if (nestedSelectItem.getWhere() != null)
            {
                visit(nestedSelectItem.getWhere(), context);
            }

            if (nestedSelectItem.getOrderBy() != null)
            {
                nestedSelectItem.getOrderBy().forEach(si -> si.accept(this, context));
            }

            projectionsAdded = true;
        }

        if (nestedSelectItem.getWhere() != null)
        {
            fromOperator = new FilterOperator(fromOperator, new ExpressionPredicate(nestedSelectItem.getWhere()));
        }

        if (nestedSelectItem.getOrderBy() != null)
        {
            // TODO: wrap from operator with order by
        }

        if (nestedSelectItem.getType() == Type.ARRAY)
        {
            context.projection = new ArrayProjection(new ArrayList<>(projections.values()), fromOperator);
        }
        else
        {
            context.projection = new ObjectProjection(projections, fromOperator);
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
        ColumnsVisitor.getColumnsByAlias(context.columnsByAlias, context.parent, expression);
    }

    @Override
    public Void visit(Join join, Context context)
    {
        join(context,
                join.getType() == JoinType.LEFT ? "LEFT" : "INNER" + " JOIN",
                join,
                join.getCondition(),
                join.getType() == JoinType.LEFT);
        return null;
    }

    @Override
    public Void visit(Apply apply, Context context)
    {
        join(context,
                apply.getType() == ApplyType.OUTER ? "OUTER" : "CROSS" + " APPLY",
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
            pushDownPredicate = analyzeResult.extractPushdownPredicate(tableSource.getAlias(), true);
            if (pushDownPredicate != null)
            {
                where = analyzeResult.getPredicate();
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
            context.operator = new FilterOperator(context.operator, new ExpressionPredicate(pushDownPredicate));
        }

        // Child joins
        tsj.getJoins().forEach(j -> j.accept(this, context));

        // Left over where after push down split, apply a filter
        if (where != null)
        {
            visit(where, context);
            context.operator = new FilterOperator(context.operator, new ExpressionPredicate(where));
        }
        // TODO: wrap context.operator with group by
        tableSource.getGroupBy().forEach(e -> visit(e, context));
        // TODO: wrap context.operator with order by
        tableSource.getOrderBy().forEach(si -> si.accept(this, context));

        // Restore values before leaving
        context.parent = parent;
        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        Pair<Catalog, QualifiedName> pair = getCatalogAndTable(context, table.getTable());
        TableAlias alias = context.appendTableAlias(pair.getValue(), table.getAlias(), null);
        if (context.tableIndex != null)
        {
            Reader reader = pair.getKey().getBatchReader(pair.getValue(), context.tableIndex);
            context.operator = new BatchOperator(alias, reader);
            context.tableIndex = null;
        }
        else
        {
            context.operator = pair.getKey().getScanOperator(alias);
        }
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        TableAlias alias = context.appendTableAlias(
                QualifiedName.of(functionInfo.getCatalog().getName(), functionInfo.getName()),
                tableFunction.getAlias(),
                functionInfo.getColumns());
        tableFunction.getArguments().forEach(a -> visit(a, context));
        context.operator = new TableFunctionOperator(alias, functionInfo, tableFunction.getArguments());
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
            Expression condition,
            boolean emitEmptyOuterRows,
            boolean isCorrelated)
    {
        AnalyzeResult analyzeResult = null;
        String innerAlias = innerTableSource.getAlias();

        // Analyze push down predicate
        if (condition != null)
        {
            // Analyze condition to see if we can hash join / merge join / utilize index
            analyzeResult = PredicateAnalyzer.analyze(condition);

            // Push down is only possible on inner joins
            if (!emitEmptyOuterRows)
            {
                Expression pushDownPredicate = analyzeResult.extractPushdownPredicate(innerAlias, true);
                if (pushDownPredicate != null)
                {
                    context.appendPushDownPredicate(innerAlias, pushDownPredicate);
                    condition = analyzeResult.getPredicate();
                }
            }
        }

        boolean populating = innerTableSource instanceof PopulateTableSource;

        List<AnalyzeItem> equiItems = analyzeResult != null ? analyzeResult.getEquiItems(innerAlias, true) : emptyList();

        // TODO: correlated

        /* No equi items in condition => NestedLoop */
        if (equiItems.isEmpty())
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

            /* Cache inner operator */
            if (!isCorrelated)
            {
                inner = new CachingOperator(inner);
            }

            return new NestedLoopJoin(
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
        Set<String> columnItems = equiItems
                .stream()
                .map(item -> item.getColumn(innerAlias, true))
                .filter(Objects::nonNull)
                .collect(toSet());

        Index index = null;

        if (!columnItems.isEmpty())
        {
            /* Find the first matching index from extracted columns above*/
            index = indices
                    .stream()
                    .filter(i -> columnItems.containsAll(i.getColumns()))
                    .findFirst()
                    .orElse(null);
        }

        // No indices for inner -> HashJoin
        if (index == null)
        {
            innerTableSource.accept(this, context);
            Operator inner = wrapWithPushDown(context, context.operator, innerAlias);

            return new HashJoin(
                    logicalOperator,
                    outer,
                    inner,
                    // Collect outer hash expressions
                    new ExpressionHashFunction(equiItems
                            .stream()
                            .map(item -> innerAlias.equals(item.getLeftAlias())
                                ? item.getRightExpression()
                                : item.getLeftExpression())
                            .collect(toList())),
                    // Collect inner hash expression
                    new ExpressionHashFunction(equiItems
                            .stream()
                            .map(item -> innerAlias.equals(item.getLeftAlias())
                                ? item.getLeftExpression()
                                : item.getRightExpression())
                            .collect(toList())),
                    new ExpressionPredicate(analyzeResult.getPredicate()),
                    DefaultRowMerger.DEFAULT,
                    populating,
                    emitEmptyOuterRows);
        }

        // Hint for index usage
        context.tableIndex = index;
        innerTableSource.accept(this, context);
        Operator inner = wrapWithPushDown(context, context.operator, innerAlias);


        
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

        throw new RuntimeException("No operator could be created for join");
    }

    private Operator wrapWithPushDown(Context context, Operator operator, String alias)
    {
        Expression pushDownPredicate = context.pushDownPredicateByAlias.remove(alias);
        /* Apply any pushdown filter */
        if (pushDownPredicate != null)
        {
            return new FilterOperator(operator, new ExpressionPredicate(pushDownPredicate));
        }
        return operator;
    }

    private Pair<Catalog, QualifiedName> getCatalogAndTable(Context context, final QualifiedName table)
    {
        requireNonNull(table, "table");
        QualifiedName tableName = table;
        Catalog catalog = context.catalogRegistry.getCatalog(tableName.getFirst());
        if (catalog == null)
        {
            catalog = context.catalogRegistry.getDefaultCatalog();
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
