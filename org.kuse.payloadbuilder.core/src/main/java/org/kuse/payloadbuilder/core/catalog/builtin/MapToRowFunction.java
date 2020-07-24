package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Table value function that extracts row from maps in target rows
 * <pre> 
 * Example
 * 
 * source row have
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
class MapToRowFunction extends TableFunctionInfo
{
    MapToRowFunction(Catalog catalog)
    {
        super(catalog, "mapToRow", Type.TABLE);
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context, TableAlias tableAlias, List<Object> arguments)
    {
        final Object value = arguments.get(0);
        final Iterator<Object> it = IteratorUtils.getIterator(value);
        return new Iterator<Row>()
        {
            int length = tableAlias.getColumns().length;
            int pos = 0;
            Row next;

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

            boolean setNext()
            {
                while (next == null)
                {
                    if (!it.hasNext())
                    {
                        return false;
                    }
                        
                    // TODO: asterisk columns
                    // TODO: type check and throw
                    @SuppressWarnings("unchecked")
                    Map<String, Object> item = (Map<String, Object>) it.next();
                    
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
