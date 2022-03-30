package org.kuse.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/**
 * Table value function that extracts row from a collection of maps in target expression
 *
 * <pre>
 *
 * Example
 *
 * SELECT field
 * FROM source s
 * OUTER APPLY
 * (
 *   open_map_collection(s.article_attribute.attribute1.buckets) a1Buckets
 *   INNER JOIN attribute1 a1
 *     ON a1.attr1_id == a1Buckets.key
 *   ORDER BY a1.attr1_code
 * ) attribute1
 * </pre>
 */
class OpenMapCollectionFunction extends TableFunctionInfo
{
    OpenMapCollectionFunction(Catalog catalog)
    {
        super(catalog, "open_map_collection");
    }

    @Override
    public String getDescription()
    {
        return "Table valued function that opens a row set from a collection of maps." + System.lineSeparator() + System.lineSeparator()
            + "Ex. " + System.lineSeparator()
            + "set @rows = '[ { \"key\": 123 }, { \"key\": 456 } ]'" + System.lineSeparator()
            + "select * from " + getName() + "(json_value(@rows)) " + System.lineSeparator() + System.lineSeparator()
            + "Will yield a row set like: " + System.lineSeparator() + System.lineSeparator()
            + "key" + System.lineSeparator()
            + "---" + System.lineSeparator()
            + "123" + System.lineSeparator()
            + "456";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return singletonList(Expression.class);
    }

    @Override
    public TupleIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        final Object value = arguments.get(0).eval(context);
        final Iterator<Object> it = CollectionUtils.getIterator(value);
        //CSOFF
        return new TupleIterator()
        //CSON
        {
            private Set<String> addedColumns;
            private String[] columns;
            private int pos;
            private Tuple next;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Tuple next()
            {
                Tuple r = next;
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

                    Object value = it.next();

                    @SuppressWarnings("unchecked")
                    Map<String, Object> item = (Map<String, Object>) value;

                    if (item == null)
                    {
                        continue;
                    }

                    if (addedColumns == null)
                    {
                        addedColumns = new LinkedHashSet<>(item.keySet());
                        columns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                    }
                    else if (addedColumns.addAll(item.keySet()))
                    {
                        columns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                    }
                    next = Row.of(tableAlias, pos++, columns, new Row.MapValues(item, columns));
                }

                return true;
            }
        };
    }
}
