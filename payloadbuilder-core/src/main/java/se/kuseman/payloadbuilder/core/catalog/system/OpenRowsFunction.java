package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.Set;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;

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
        return "Table valued function that opens a row set from table alias expression." + System.lineSeparator()
               + System.lineSeparator()
               + "Ex. "
               + System.lineSeparator()
               + "SELECT a.col"
               + System.lineSeparator()
               + ",      ("
               + System.lineSeparator()
               + "          SELECT b.col,"
               + System.lineSeparator()
               + "                 b.col2"
               + System.lineSeparator()
               + "          FROM open_rows(b)"
               + System.lineSeparator()
               + "          FOR OBJECT_ARRAY"
               + System.lineSeparator()
               + "       )     collection"
               + System.lineSeparator()
               + "FROM tableA a"
               + System.lineSeparator()
               + "INNER JOIN tableB b WITH (populate=true)"
               + System.lineSeparator()
               + "  on b.id = a.id"
               + System.lineSeparator();
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public Set<TableAlias> resolveAlias(TableAlias alias, Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Result is the input
        return argumentAliases.get(0);
    }

    @Override
    public TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments)
    {
        Object value = arguments.get(0)
                .eval(context);
        if (value == null)
        {
            return TupleIterator.EMPTY;
        }
        else if (value instanceof TupleIterator)
        {
            return (TupleIterator) value;
        }

        throw new IllegalArgumentException("Expected a tuple iterator from argument to " + getName() + " but got: " + value);
    }
}
