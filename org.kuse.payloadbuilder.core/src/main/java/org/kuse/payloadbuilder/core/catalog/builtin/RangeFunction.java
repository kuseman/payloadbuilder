package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.stream.IntStream;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Range table valued function that emits row in range */
class Range extends TableFunctionInfo
{
    private static final String[] COLUMNS = new String[] {"Value"};

    Range(Catalog catalog)
    {
        super(catalog, "range");
    }

    @Override
    public String[] getColumns()
    {
        return COLUMNS;
    }

    @Override
    public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
            to = ((Number) requireNonNull(arguments.get(1).eval(context), "To argument to range cannot be null.")).intValue();
        }
        return RowIterator.wrap(IntStream.range(from, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] {i})).iterator());
    }
}
