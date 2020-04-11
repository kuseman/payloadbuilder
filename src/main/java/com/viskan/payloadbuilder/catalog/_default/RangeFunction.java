package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.OperatorContext;

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
    public Iterator<Row> open(OperatorContext context, TableAlias tableAlias, List<Object> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) arguments.get(0)).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) arguments.get(0)).intValue();
            to = ((Number) arguments.get(1)).intValue();
        }
        return IntStream.range(from, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] {i})).iterator();
    }
}
