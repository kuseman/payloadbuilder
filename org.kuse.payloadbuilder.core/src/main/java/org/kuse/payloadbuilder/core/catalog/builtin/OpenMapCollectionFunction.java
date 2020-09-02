package org.kuse.payloadbuilder.core.catalog.builtin;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Table value function that extracts row from a collection of maps in target expression
 * <pre> 
 * Example
 * 
 * SELECT field
 * FROM source s
 * OUTER APPLY
 * [
 *   mapToRow(s.article_attribute.attribute1.buckets) a1Buckets
 *   INNER JOIN [attribute1] a1
 *     ON a1.attr1_id == a1Buckets.key
 *   ORDER BY a1.attr1_code
 * ] attribute1
 * 
 * </pre>
 * */
class OpenMapCollectionFunction extends TableFunctionInfo
{
    OpenMapCollectionFunction(Catalog catalog)
    {
        super(catalog, "open_map_collection");
    }
    
    @Override
    public String getDescription()
    {
        return "Table valued function that opens a row set from a collection of maps." + System.lineSeparator() + System.lineSeparator() +
                "Ex. " + System.lineSeparator() + 
                "set @rows = '[ { \"key\": 123 }, { \"key\": 456 } ]'" + System.lineSeparator() +
                "select * from " + getName() + "(json_value(@rows)) " + System.lineSeparator() + System.lineSeparator() + 
                "Will yield a row set like: " + System.lineSeparator() + System.lineSeparator() +
                "key" + System.lineSeparator() +
                "---" + System.lineSeparator() +
                "123" + System.lineSeparator() +
                "456";
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        final Object value = arguments.get(0).eval(context);
        final Iterator<Object> it = IteratorUtils.getIterator(value);
        return new Iterator<>()
        {
            private Set<String> addedColumns;
            private int pos = 0;
            private Row next;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!it.hasNext())
                    {
                        return false;
                    }
                        
                    @SuppressWarnings("unchecked")
                    Map<String, Object> item = (Map<String, Object>) it.next();
                    
                    if (tableAlias.isAsteriskColumns())
                    {
                        if (addedColumns == null)
                        {
                            addedColumns = new LinkedHashSet<>(item.keySet());
                            tableAlias.setColumns(addedColumns.toArray(EMPTY_STRING_ARRAY));
                        }
                        else if (addedColumns.addAll(item.keySet()))
                        {
                            tableAlias.setColumns(addedColumns.toArray(EMPTY_STRING_ARRAY));
                        }
                    }
                    
                    int length = ArrayUtils.getLength(tableAlias.getColumns());
                    Object[] values = new Object[length];
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = item.get(tableAlias.getColumns()[i]);
                    }

                    next = Row.of(tableAlias, pos++, values);
                }

                return next != null;
            }
        };
    }
}
