package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.OperatorContext;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;

/** Table value function that extracts row from maps in target rows
 * <pre> 
 * Example
 * 
 * source row have
 * 
 * SELECT field
 * FROM source s
 * OUTER APPLY mapToRow(s.article_attribute.attribute1.buckets) attribute1Buckets (
 *   INNER JOIN attribute1 a1
 *      ON a1.attr1_id == attribute1Buckets.key
 * )
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
    public Iterator<Row> open(OperatorContext context, TableAlias tableAlias, List<Object> arguments)
    {
        final Object value = arguments.get(0);
        final Iterator it = IteratorUtils.getIterator(value);
        return new Iterator<Row>()
        {
            String[] columns;
            int pos = 0;
            Row next;

            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
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
                        
                    // TODO: type check and throw
                    @SuppressWarnings("unchecked")
                    Map<String, Object> item = (Map<String, Object>) it.next();

                    if (columns == null)
                    {
                        columns = item.keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
                        tableAlias.setColumns(columns);
                    }

                    int length = columns.length;
                    Object[] values = new Object[length];
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = item.get(columns[i]);
                    }

                    next = Row.of(tableAlias, pos++, values);
                }

                return next != null;
            }
        };
    }
}
