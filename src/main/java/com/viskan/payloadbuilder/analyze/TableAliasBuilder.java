package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem;
import com.viskan.payloadbuilder.parser.tree.PopulateTableSource;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableFunction;
import com.viskan.payloadbuilder.parser.tree.TableSource;
import com.viskan.payloadbuilder.parser.tree.TableSourceJoined;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import gnu.trove.map.hash.THashMap;

/** Build a table alias hierarchy from a {@link Query} */
public class TableAliasBuilder extends ATreeVisitor<Void, TableAliasBuilder.Context>
{
    private static final TableAliasBuilder VISITOR = new TableAliasBuilder();

    private TableAliasBuilder()
    {
    }

    static class Context
    {
        Map<TableAlias, Set<String>> columnsByAlias = new THashMap<>();
        TableAlias parent;
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

    /** Build alias hierarchy from query */
    public static TableAlias build(Query query)
    {
        Context ctx = new Context();
        query.accept(VISITOR, ctx);
        ctx.columnsByAlias.entrySet().forEach(e -> e.getKey().setColumns(e.getValue().toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
        
        TableAlias result = ctx.parent;
        while (result.getParent() != null)
        {
            result = result.getParent();
        }
        
        return result;
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
    protected void visit(Expression expression, Context context)
    {
        // Resolve columns
        ColumnsVisitor.getColumnsByAlias(context.columnsByAlias, context.parent, expression);
    }

    @Override
    public Void visit(Table table, Context context)
    {
        context.appendTableAlias(table.getTable(), table.getAlias());
        return null;
    }
    
    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        TableFunctionInfo functionInfo = tableFunction.getFunctionInfo();
        context.appendTableAlias(QualifiedName.of(functionInfo.getCatalog().getName(), functionInfo.getName()), tableFunction.getAlias());
        return super.visit(tableFunction, context);
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
        
        populatingJoin.getGroupBy().forEach(e -> visit(e, context));
        if (populatingJoin.getWhere() != null)
        {
            visit(populatingJoin.getWhere(), context);
        }
        populatingJoin.getOrderBy().forEach(o -> o.accept(this, context));
        
        context.parent = parent;
        return null;
    }
}
