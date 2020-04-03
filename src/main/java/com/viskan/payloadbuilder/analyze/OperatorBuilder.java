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
import com.viskan.payloadbuilder.operator.ExpressionOperator;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.Filter;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.RowSpool;
import com.viskan.payloadbuilder.operator.RowSpoolScan;
import com.viskan.payloadbuilder.parser.tree.AJoin;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Apply;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.ExpressionSelectItem;
import com.viskan.payloadbuilder.parser.tree.Join;
import com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression;
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
        CatalogRegistry catalogRegistry;
        TableAlias parent;
        Map<TableAlias, Set<String>> columnsByAlias = new THashMap<>();

        /** Resulting operator */
        Operator operator;

        /** Resulting projection */
        Projection projection;

        /**
         * Alias that should override provided alias to {@link #appendTableAlias(QualifiedName, String)} Used in populating joins
         */
        String alias;
        /** Should new parent be set upon calling {@link #appendTableAlias(QualifiedName, String)} */
        boolean setParent;
        /**
         * Flag that says if parent joins should be spooled. This is needed to let table implementations make use of parent rows when fetching data.
         * TODO: Move this to a catalog/operatorfactory feature
         */
        boolean spoolParents;

        /** Counter for spool key used when having outer populating joins to store non matching rows */
        int populatingSpoolKeyIndex;

        /** Predicate pushed down from join to table source */
        Expression pushedDownPredicate;

        TableAlias appendTableAlias(QualifiedName table, String alias)
        {
            String aliasToUse = this.alias != null ? this.alias : alias;

            if (parent == null)
            {
                parent = TableAlias.of(null, table, aliasToUse);
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
        context.columnsByAlias.entrySet().forEach(e -> e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY)));
        return Pair.of(context.operator, context.projection);
    }

    @Override
    public Void visit(Query query, Context context)
    {
        TableSourceJoined tsj = query.getFrom();
        tsj.getTableSource().accept(this, context);

        List<AJoin> joins = tsj.getJoins();
        int size = joins.size();
        for (int i = 0; i < size; i++)
        {
            joins.get(i).accept(this, context);
        }

        if (query.getWhere() != null)
        {
            context.operator = new Filter(context.operator, new ExpressionPredicate(query.getWhere()));
            visit(query.getWhere(), context);
        }

        // TODO: wrap context.operator with order by
        query.getOrderBy().forEach(o -> o.accept(this, context));
        // TODO: wrap context.operator with group by
        query.getGroupBy().forEach(e -> visit(e, context));

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

            projectionsAdded = true;

            if (nestedSelectItem.getWhere() != null)
            {
                fromOperator = new Filter(fromOperator, new ExpressionPredicate(nestedSelectItem.getWhere()));
                visit(nestedSelectItem.getWhere(), context);
            }

            if (nestedSelectItem.getOrderBy() != null)
            {
                // TODO: wrap from operator with order by
                nestedSelectItem.getOrderBy().forEach(si -> si.accept(this, context));
            }
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

        // Analyze expression to see if there is push down candidates
        Pair<Expression, Expression> pushDownPair = PredicateVisitor.analyze(join.getCondition(), tableSource.getAlias());
        if (pushDownPair.getKey() != null)
        {
            context.pushedDownPredicate = pushDownPair.getKey();
            condition = pushDownPair.getValue();
            if (condition == null)
            {
                condition = LiteralBooleanExpression.TRUE_LITERAL;
            }
        }

        tableSource.accept(this, context);
        Operator inner = context.operator;

        // If inner operator needs parents spooled, then reuse that spool for outer operator to
        Operator joinOuter = context.spoolParents ? RowSpoolScan.PARENTS_SPOOL_SCAN : outer;
        Operator joinInner = inner;

        // TODO: Code generation
        // TODO: emit NULL rows (LEFT JOIN)
        Operator joinOperator = createJoin(
                joinOuter,
                joinInner,
                condition,
                tableSource instanceof PopulateTableSource);

        visit(join.getCondition(), context);
        context.operator = context.spoolParents ? new RowSpool("parents", outer, joinOperator) : joinOperator;
        return null;
    }

    @Override
    public Void visit(Apply apply, Context context)
    {
        //        throw new NotImplementedException();
        //        Operator outer = context.operator;
        TableSource tableSource = apply.getTableSource();
        tableSource.accept(this, context);
        //        Operator inner = context.operator;
        //
        //        // Apply => nested loop without predicate.
        //        // TODO: emit NULL rows (OUTER APPLY)
        //        // TODO: might need an apply operator instead of nested loop
        //        context.operator = new NestedLoop(outer, new CachingOperator(inner), (eCtx, row) -> true, false);
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
            Pair<Expression, Expression> pushDownPair = PredicateVisitor.analyze(where, tableSource.getAlias());
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
        // TODO: wrap context.operator with order by etc.
        tableSource.getOrderBy().forEach(o -> o.accept(this, context));

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

        TableAlias tableAlias = context.appendTableAlias(tableName, table.getAlias());

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
        context.appendTableAlias(QualifiedName.of(functionInfo.getCatalog().getName(), functionInfo.getName()), tableFunction.getAlias());
        tableFunction.getArguments().forEach(a -> visit(a, context));

        final Operator outer = context.operator;
        // TODO: How to provide outer row and evaluate arguments to function
        //       Detect correlated, if so use nested loop
        context.spoolParents = false;
        context.operator = context1 -> null;

        return null;
    }

    private Operator createJoin(
            Operator outer,
            Operator inner,
            Expression condition,
            boolean populating)
    {
        // TODO: hash join detector, merge join detector

        // Nested loop needs a cache around inner operator
        // due to multiple loops
        // But if the inner is a spool then it's already cached
        Operator innerOperator = inner;
        if (!(inner instanceof RowSpoolScan))
        {
            innerOperator = new CachingOperator(innerOperator);
        }

        BiPredicate<EvaluationContext, Row> predicate = condition != null ? new ExpressionPredicate(condition) : ExpressionPredicate.TRUE;
        Operator join = new NestedLoop(
                outer,
                innerOperator,
                predicate,
                DefaultRowMerger.DEFAULT,
                populating);

        return join;
    }
}
