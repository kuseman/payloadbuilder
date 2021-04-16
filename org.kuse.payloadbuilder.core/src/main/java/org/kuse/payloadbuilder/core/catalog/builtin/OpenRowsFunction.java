package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/**
 * Table value function that extracts rows from an expression Usually used when accssing table aliases from within sub query expressions
 *
 * <pre>
 *
 * Example
 *
 * SELECT a.col
 * ,      (
 *           select b.col,
 *                  b.col2
 *           from open_rows(b)
 *           for object_array
 *        )     collection
 * FROM tableA a
 * inner join tableB b with (populate=true)
 *   on b.id = a.id
 * </pre>
 */
class OpenRowsFunction extends TableFunctionInfo
{
    OpenRowsFunction(Catalog catalog)
    {
        super(catalog, "open_rows");
    }

    @Override
    public String getDescription()
    {
        return "Table valued function that opens a row set from table alias expression." + System.lineSeparator() + System.lineSeparator()
            + "Ex. " + System.lineSeparator()
            + "SELECT a.col" + System.lineSeparator()
            + ",      (" + System.lineSeparator()
            + "          SELECT b.col," + System.lineSeparator()
            + "                 b.col2" + System.lineSeparator()
            + "          FROM open_rows(b)" + System.lineSeparator()
            + "          FOR OBJECT_ARRAY" + System.lineSeparator()
            + "       )     collection" + System.lineSeparator()
            + "FROM tableA a" + System.lineSeparator()
            + "INNER JOIN tableB b WITH (populate=true)" + System.lineSeparator()
            + "  on b.id = a.id" + System.lineSeparator();
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return singletonList(Expression.class);
    }

    @Override
    public Set<TableAlias> resolveAlias(TableAlias alias, Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Result is the input
        return argumentAliases.get(0);
    }

    @Override
    public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        final Object value = arguments.get(0).eval(context);
        final Iterator<Object> it = CollectionUtils.getIterator(value);
        //CSOFF
        return new RowIterator()
        //CSON
        {
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
                    next = (Tuple) value;
                }

                return next != null;
            }
        };
    }
}
