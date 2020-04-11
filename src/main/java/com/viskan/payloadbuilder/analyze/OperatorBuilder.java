package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.OperatorFactory;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.operator.ArrayProjection;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionHashFunction;
import com.viskan.payloadbuilder.operator.ExpressionOperator;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.Filter;
import com.viskan.payloadbuilder.operator.HashMatch;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.RowSpool;
import com.viskan.payloadbuilder.operator.RowSpoolScan;
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
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.Pair;

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

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

        /**
         * Flag that says if parent joins should be spooled. This is needed to let table implementations make use of parent rows when fetching data.
         */
        private boolean spoolParents;

        /** Predicate pushed down from join to table source */
        private Expression pushedDownPredicate;

        /** Appends table alias to hierarchy */
        private TableAlias appendTableAlias(QualifiedName table, String alias, String[] columns)
        {
            String aliasToUse = this.alias != null ? this.alias : alias;

            if (parent == null)
            {
                parent = TableAlias.of(null, table, aliasToUse);
                if (columns != null)
                {
                    parent.setColumns(columns);
                }
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

                TableAlias newAlias = TableAlias.of(parent, table, aliasToUse);
                if (columns != null)
                {
                    newAlias.setColumns(columns);
                }
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
        Expression pushedDownPredicate = null;
        if (where != null)
        {
            /* TODO:
              Push down can applied to non populating joins ie:
              select s.id
              from source s
              inner join article a
                on a.id = s.id
              inner join brand b
                on b.id = a.bid
              where s.id > 0        <--- Pusehd down soutce
              and a.id2 < 10        <--- Can be pushed to article
              and b.id3 != 10       <--- can be pushed to brand
            
            */
            Pair<Expression, Expression> pushDownPair = PushDownPredicateVisitor.analyze(where, tsj.getTableSource().getAlias());
            if (pushDownPair != null)
            {
                pushedDownPredicate = pushDownPair.getKey();
                context.pushedDownPredicate = pushedDownPredicate;
                where = pushDownPair.getValue();
            }
        }

        tsj.getTableSource().accept(this, context);

        // Collect columns from pushed down predicate
        if (pushedDownPredicate != null)
        {
            visit(pushedDownPredicate, context);
        }

        List<AJoin> joins = tsj.getJoins();
        int size = joins.size();
        for (int i = 0; i < size; i++)
        {
            joins.get(i).accept(this, context);
        }

        if (where != null)
        {
            context.operator = new Filter(context.operator, new ExpressionPredicate(where));
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
            fromOperator = new Filter(fromOperator, new ExpressionPredicate(nestedSelectItem.getWhere()));
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
        Operator outer = context.operator;
        TableSource tableSource = join.getTableSource();

        Expression condition = join.getCondition();

        if (join.getType() == JoinType.INNER)
        {
            // TODO: Push down to outer alias
            /*  Select source s
             *  inner join article a
             *    on  a.art_id = s.art_id
             *    and s.active_flg  <----- can be pushed into source
             *
             *
             */
            // Analyze expression to see if there is push down candidates
            Pair<Expression, Expression> pushDownPair = PushDownPredicateVisitor.analyze(join.getCondition(), tableSource.getAlias());
            if (pushDownPair.getKey() != null)
            {
                context.pushedDownPredicate = pushDownPair.getKey();
                condition = pushDownPair.getValue();
            }
        }

        boolean isCorrelated = detectCorrelated(tableSource, context);

        Operator inner = context.operator;

        // If inner operator needs parents spooled, then reuse that spool for outer operator to
        Operator joinOuter = context.spoolParents ? RowSpoolScan.PARENTS_SPOOL_SCAN : outer;
        Operator joinInner = inner;

        // TODO: Code generation
        Operator joinOperator = createJoin(
                join.getType() == JoinType.LEFT ? "LEFT" : "INNER" + " JOIN",
                tableSource.getAlias(),
                joinOuter,
                joinInner,
                condition,
                tableSource instanceof PopulateTableSource,
                join.getType() == JoinType.LEFT,
                isCorrelated);

        visit(join.getCondition(), context);
        context.operator = context.spoolParents ? new RowSpool("parents", outer, joinOperator) : joinOperator;
        return null;
    }

    @Override
    public Void visit(Apply apply, Context context)
    {
        Operator outer = context.operator;
        TableSource tableSource = apply.getTableSource();

        boolean isCorrelated = detectCorrelated(tableSource, context);

        // TODO: context.isCorrelated should be set if tableSource is correlated

        Operator inner = context.operator;

        // If inner operator needs parents spooled, then reuse that spool for outer operator to
        Operator joinOuter = context.spoolParents ? RowSpoolScan.PARENTS_SPOOL_SCAN : outer;
        Operator joinInner = inner;

        Operator joinOperator = createJoin(
                apply.getType() == ApplyType.OUTER ? "OUTER" : "CROSS" + " APPLY",
                tableSource.getAlias(),
                joinOuter,
                joinInner,
                null,   // Apply does not have any condition
                tableSource instanceof PopulateTableSource,
                apply.getType() == ApplyType.OUTER,
                isCorrelated);

        context.operator = context.spoolParents ? new RowSpool("parents", outer, joinOperator) : joinOperator;
        return null;
    }

    @Override
    public Void visit(PopulateTableSource tableSource, Context context)
    {
        // Store current parent alias
        TableAlias parent = context.parent;
        TableSourceJoined tsj = tableSource.getTableSourceJoined();

        Expression where = tableSource.getWhere();
        Expression pushedDownPredicate = null;
        if (where != null)
        {
            // Analyze expression to see if there is push down candidates
            Pair<Expression, Expression> pushDownPair = PushDownPredicateVisitor.analyze(where, tableSource.getAlias());
            if (pushDownPair.getKey() != null)
            {
                pushedDownPredicate = pushDownPair.getKey();

                // Merge already existing pushed down predicate if any
                if (context.pushedDownPredicate != null)
                {
                    context.pushedDownPredicate = new LogicalBinaryExpression(
                            LogicalBinaryExpression.Type.AND,
                            context.pushedDownPredicate,
                            pushDownPair.getKey());
                }
                else
                {
                    context.pushedDownPredicate = pushDownPair.getKey();
                }

                where = pushDownPair.getValue();
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
        if (pushedDownPredicate != null)
        {
            visit(pushedDownPredicate, context);
        }

        // Remember spool parents for the table source
        // before processing joins
        // and re-set it again when leaving to let upstream detect it correctly
        boolean spoolParents = context.spoolParents;

        // Child joins
        tsj.getJoins().forEach(j -> j.accept(this, context));

        // Left over where after push down split, apply a filter
        if (where != null)
        {
            visit(where, context);
            context.operator = new Filter(context.operator, new ExpressionPredicate(where));
        }
        // TODO: wrap context.operator with group by
        tableSource.getGroupBy().forEach(e -> visit(e, context));
        // TODO: wrap context.operator with order by
        tableSource.getOrderBy().forEach(si -> si.accept(this, context));

        // Restore parent before leaving
        context.parent = parent;
        // Restore spool parents setting before leaving
        context.spoolParents = spoolParents;
        return null;
    }

    @Override
    public Void visit(Table table, Context context)
    {
        QualifiedName tableName = table.getTable();
        Catalog catalog = context.catalogRegistry.getCatalog(tableName.getFirst());
        if (catalog == null)
        {
            catalog = context.catalogRegistry.getDefault();
        }
        else
        {
            // Remove catalog name from table name
            tableName = tableName.extract(1, tableName.getParts().size());
        }

        TableAlias tableAlias = context.appendTableAlias(tableName, table.getAlias(), null);

        OperatorFactory operatorFactory = catalog.getOperatorFactory();
        if (operatorFactory == null)
        {
            throw new IllegalArgumentException("No operator factory registerd for catalog: " + catalog.getName());
        }

        context.spoolParents = operatorFactory.requiresParents(tableName);
        context.operator = operatorFactory.create(tableName, tableAlias);
        if (context.pushedDownPredicate != null)
        {
            context.operator = new Filter(context.operator, new ExpressionPredicate(context.pushedDownPredicate));
            context.pushedDownPredicate = null;
        }
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        TableAlias tableAlias = context.appendTableAlias(
                QualifiedName.of(functionInfo.getCatalog().getName(), functionInfo.getName()),
                tableFunction.getAlias(),
                functionInfo.getColumns());
        tableFunction.getArguments().forEach(a -> visit(a, context));
        context.spoolParents = false;
        context.operator = new TableFunctionOperator(tableAlias, functionInfo, tableFunction.getArguments());
        return null;
    }

    private boolean detectCorrelated(TableSource tableSource, Context context)
    {
        // Store aliases before visiting table source to be able to detect if this join is correlated
        Map<TableAlias, Set<String>> columnsByAlias = context.columnsByAlias;
        context.columnsByAlias = new THashMap<>();

        // If there are any references to current parent
        // or any parent or parents children, then table source is correlated
        Set<TableAlias> parents = new THashSet<>();
        parents.add(context.parent);
        TableAlias current = context.parent.getParent();
        while (current != null)
        {
            parents.add(current);
            parents.addAll(current.getChildAliases());
            current = current.getParent();
        }

        tableSource.accept(this, context);

        boolean isCorrelated = context.columnsByAlias.keySet().stream().anyMatch(alias -> parents.contains(alias));

        columnsByAlias.putAll(context.columnsByAlias);
        context.columnsByAlias = columnsByAlias;

        return isCorrelated;
    }

    private Operator createJoin(
            String logicalOperator,
            String innerAlias,
            Operator outer,
            Operator inner,
            Expression condition,
            boolean populating,
            boolean emitEmptyOuterRows,
            boolean isCorrelated)
    {
        // TODO: hash join detector, merge join detector

        if (condition != null && !isCorrelated)
        {
            Pair<List<Expression>, List<Expression>> hashJoinPair = HashJoinDetector.detect(condition, innerAlias);

            if (!hashJoinPair.getKey().isEmpty())
            {
                return new HashMatch(
                        logicalOperator,
                        outer,
                        inner,
                        new ExpressionHashFunction(hashJoinPair.getValue()),
                        new ExpressionHashFunction(hashJoinPair.getKey()),
                        new ExpressionPredicate(condition),
                        DefaultRowMerger.DEFAULT,
                        populating,
                        emitEmptyOuterRows);
            }
        }

        // Nested loop needs a cache around inner operator
        // due to multiple loops
        // But if the inner is a spool then it's already cached
        // If this is a correlated join, then an inner cache is also impossible
        Operator innerOperator = inner;
        if (!(inner instanceof RowSpoolScan) && !(inner instanceof CachingOperator) && !isCorrelated)
        {
            innerOperator = new CachingOperator(innerOperator);
        }

        BiPredicate<EvaluationContext, Row> predicate = condition != null ? new ExpressionPredicate(condition) : null;
        Operator join = new NestedLoop(
                logicalOperator,
                outer,
                innerOperator,
                predicate,
                DefaultRowMerger.DEFAULT,
                populating,
                emitEmptyOuterRows);

        return join;
    }
}
