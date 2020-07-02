package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

/** Range table valued function that emits row in range */
class Range extends TableFunctionInfo
{
    private static final String[] COLUMNS = new String[] {"Value"};

    Range(Catalog catalog)
    {
        super(catalog, "range", Type.TABLE);
    }

    @Override
    public String[] getColumns()
    {
        return COLUMNS;
    }

    @Override
    public Iterator<Row> open(ExecutionContext context, TableAlias tableAlias, List<Object> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) requireNonNull(arguments.get(0), "From argument to range cannot be null.")).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) requireNonNull(arguments.get(0), "From argument to range cannot be null.")).intValue();
            to = ((Number) requireNonNull(arguments.get(1), "To argument to range cannot be null.")).intValue();
        }
        return IntStream.range(from, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] {i})).iterator();
    }
}
