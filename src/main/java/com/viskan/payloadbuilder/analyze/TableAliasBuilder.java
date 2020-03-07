package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.JoinedTableSource;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableFunction;
import com.viskan.payloadbuilder.parser.tree.TableSource;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static java.util.Arrays.asList;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import gnu.trove.map.hash.THashMap;

/** Build a table alias hierarchy from a {@link Query} */
public class TableAliasBuilder extends ATreeVisitor<Void, TableAliasBuilder.Context>
{
    private static final TableAliasBuilder VISITOR = new TableAliasBuilder();
    
    private TableAliasBuilder()
    {}
    
    static class Context
    {
        Map<TableAlias, Set<String>> columnsByAlias = new THashMap<>();
        TableAlias parent;

        void appendTableAlias(QualifiedName table, String alias)
        {
            if (parent == null)
            {
                parent = TableAlias.of(null, table, alias);
            }
            else
            {
                if (parent.getChildAlias(alias) != null)
                {
                    throw new IllegalArgumentException("Alias " + alias + " already exists as child to: " + parent);
                }
                
                TableAlias current = parent;
                while (current != null)
                {
                    if (alias.equals(current.getAlias()))
                    {
                        throw new IllegalArgumentException("Alias " + alias + " already exists in parent hierarchy from: " + parent);
                    }
                    current = current.getParent();
                }
                
                parent = TableAlias.of(parent, table, alias);
            }
        }
    }
    
    /** Build alias hierarchy from query */
    public static TableAlias build(Query query)
    {
        Context ctx = new Context();
        query.accept(VISITOR, ctx);
        ctx.columnsByAlias.entrySet().forEach(e -> e.getKey().setColumns(e.getValue().toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
        return ctx.parent;
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
    public Void visit(JoinedTableSource joinedTableSource, Context context)
    {
        TableSource ts = joinedTableSource.getTableSource();
        String alias = ts.getAlias();
        QualifiedName table;
        if (ts instanceof Table)
        {
            table = ((Table) ts).getTable();
        }
        else
        {
            TableFunction tf = (TableFunction) ts;
            table = new QualifiedName(asList(tf.getFunctionInfo().getCatalog().getName(), tf.getFunctionInfo().getName()));
        }
        context.appendTableAlias(table, alias);
        
        super.visit(joinedTableSource, context);
        
        // Traverse up when this join is complete
        if (context.parent != null && context.parent.getParent() != null)
        {
            context.parent = context.parent.getParent();
        }

        return null;
    }
}
