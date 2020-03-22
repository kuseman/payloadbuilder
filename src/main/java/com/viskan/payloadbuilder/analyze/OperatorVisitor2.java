package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.OperatorFactory;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Apply;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.ExpressionSelectItem;
import com.viskan.payloadbuilder.parser.tree.Join;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem;
import com.viskan.payloadbuilder.parser.tree.PopulateTableSource;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableFunction;
import com.viskan.payloadbuilder.parser.tree.TableSource;
import com.viskan.payloadbuilder.parser.tree.TableSourceJoined;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;

import gnu.trove.map.hash.THashMap;

/** Operator that creates an operator from a query */
public class OperatorVisitor2 extends ATreeVisitor<Void, OperatorVisitor2.Context>
{
    private static final OperatorVisitor2 VISITOR = new OperatorVisitor2();
    
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
        
        /** Alias that should override provided alias to {@link #appendTableAlias(QualifiedName, String)}
         * Used in populating joins */
        String alias;
        
        void appendTableAlias(QualifiedName table, String alias)
        {
            String aliasToUse = this.alias != null ? this.alias : alias;
            
            if (parent == null)
            {
                parent = TableAlias.of(null, table, aliasToUse);
            }
            else
            {
                if (parent.getChildAlias(aliasToUse) != null)
                {
                    throw new IllegalArgumentException("Alias " + aliasToUse + " already exists as child to: " + parent);
                }

                TableAlias current = parent;
                while (current != null)
                {
                    if (alias.equals(current.getAlias()))
                    {
                        throw new IllegalArgumentException("Alias " + aliasToUse + " already exists in parent hierarchy from: " + parent);
                    }
                    current = current.getParent();
                }

                parent = TableAlias.of(parent, table, aliasToUse);
            }
        }
    }
    
    /** Create operator */
    public static Operator create(CatalogRegistry catalogRegistry, Query query)
    {
        Context context = new Context();
        context.catalogRegistry = catalogRegistry;
        query.accept(VISITOR, context);
        context.columnsByAlias.entrySet().forEach(e -> e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY)));
        return context.operator;
    }
    
    /*
     * 
op0 = spool("parents", scan(source))

op1 = spool("parents", join(op0, scanWithSpool("parents", article)))

-- (aa)

op2 = scanWithSpool("parents", articleAttribute)
op3 = spool("parents", join(op2, scanWithSpool("parents", articlePrice)))

op4 = join(op3, scanWithSpool("parents", attribute1))
    
-- (/aa)

op5 = join(op1, op4)

-------

op0 = scan(source)

op1 = scan(article)
op2 = join(op0, op1)

-- (aa)

op3 = scan(articleAttribute)
op4 = scan(aticlePrice)
op5 = join(op3, op4)

-- (/aa)

op6 = join(op2, op5)



     */
    
    @Override
    public Void visit(Query query, Context context)
    {
        query.getFrom().accept(this, context);
        if (query.getWhere() != null)
        {
            visit(query.getWhere(), context);
        }
        query.getOrderBy().forEach(o -> o.accept(this, context));
        query.getGroupBy().forEach(e -> visit(e, context));
        
        // Reset alias to root before processing select items
        while (context.parent.getParent() != null)
        {
            context.parent = context.parent.getParent();
        }
        
        query.getSelectItems().forEach(s -> s.accept(this, context));
        return null;
    }
    
    @Override
    public Void visit(NestedSelectItem nestedSelectItem, Context context)
    {
        Set<TableAlias> aliases = asSet(context.parent);
        if (nestedSelectItem.getFrom() != null)
        {
            aliases = ColumnsVisitor.getColumnsByAlias(context.columnsByAlias, context.parent, nestedSelectItem.getFrom());
        }

        // Use found aliases and traverse select items, order and where
        TableAlias parent = context.parent;
        for (TableAlias alias : aliases)
        {
            context.parent = alias;
            nestedSelectItem.getSelectItems().forEach(s -> s.accept(this, context));

            if (nestedSelectItem.getWhere() != null)
            {
                visit(nestedSelectItem.getWhere(), context);
            }

            if (nestedSelectItem.getOrderBy() != null)
            {
                nestedSelectItem.getOrderBy().forEach(si -> si.accept(this, context));
            }
        }
        
        context.parent = parent;
        return null;
    }
    
    @Override
    public Void visit(ExpressionSelectItem expressionSelectItem, Context context)
    {
        context.projection = (writer, row) ->
        {
            Object object = expressionSelectItem.getExpression().eval(null, row);
            writer.writeFieldName(expressionSelectItem.getIdentifier());
            writer.writeValue(object);
        };
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
        // TODO: detect physical join operator (hashjoin, merge join)
        //       push down of predicate to table source
        //       does inner need outer rows in a RowSpool?
        //       wrap outer with a row spool and inner with a spool scan
        
        Operator outer = context.operator;
        TableSource tableSource = join.getTableSource();
        tableSource.accept(this, context);
        Operator inner = context.operator;
        
        if (!tableSource.isPopulating())
        {
            System.out.println("Clear parent rows!");
        }
        
        // TODO: Code generation
        // TODO: emit NULL rows (LEFT JOIN)
        context.operator = new NestedLoop(outer, new CachingOperator(inner), row -> BooleanUtils.isTrue((Boolean) join.getCondition().eval(null, row)), tableSource.isPopulating());
        visit(join.getCondition(), context);
        return null;
    }
    
    @Override
    public Void visit(Apply apply, Context context)
    {
        Operator outer = context.operator;
        TableSource tableSource = apply.getTableSource();
        tableSource.accept(this, context);
        Operator inner = context.operator;
        
        if (!tableSource.isPopulating())
        {
            System.out.println("Clear parent rows!");
        }
        
        // Apply => nested loop without predicate. 
        // TODO: emit NULL rows (OUTER APPLY)
        // TODO: might need an apply operator instead of nested loop
        context.operator = new NestedLoop(outer, new CachingOperator(inner), row -> true, tableSource.isPopulating());
        return null;
    }
    
    @Override
    public Void visit(PopulateTableSource populatingJoin, Context context)
    {
        TableAlias parent = context.parent;
        TableSourceJoined tsj = populatingJoin.getTableSourceJoined();
        TableSource ts = tsj.getTableSource();
        
        context.alias = populatingJoin.getAlias();
        ts.accept(this, context);
        context.alias = null;
        
        tsj.getJoins().forEach(j -> j.accept(this, context));

        Operator populateOperator = context.operator;
        
        populatingJoin.getGroupBy().forEach(e -> visit(e, context));
        if (populatingJoin.getWhere() != null)
        {
            visit(populatingJoin.getWhere(), context);
        }
        populatingJoin.getOrderBy().forEach(o -> o.accept(this, context));
        
        // TODO: wrap populateOperator with order by etc.
        
        context.parent = parent;
        return null;
    }
    
    @Override
    public Void visit(Table table, Context context)
    {
        context.appendTableAlias(table.getTable(), table.getAlias());
        
        Catalog catalog = context.catalogRegistry.getCatalog(table.getTable().getFirst());
        if (catalog == null)
        {
            catalog = context.catalogRegistry.getDefault();
        }
        
        OperatorFactory operatorFactory = catalog.getOperatorFactory();
        if (operatorFactory == null)
        {
            throw new IllegalArgumentException("No operator factory registerd for catalog: " + catalog.getName());
        }
        
        context.operator = operatorFactory.create(context.parent);
        return null;
    }
    
    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        context.appendTableAlias(QualifiedName.of(functionInfo.getCatalog().getName() , functionInfo.getName()), tableFunction.getAlias());
        tableFunction.getArguments().forEach(a -> visit(a, context));
        
        final Operator outer = context.operator;
        // TODO: How to provide outer row and evaluate arguments to function
        context.operator = context1 -> null;
        
        return null;
    }
    
}
