package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.EvalUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Function listOf. Creates a list of provided arguments */
class ListOfFunction extends ScalarFunctionInfo
{
    ListOfFunction(Catalog catalog)
    {
        super(catalog, "listOf");
    }

    @Override
    public String getDescription()
    {
        return "Creates a list of provided arguments." + System.lineSeparator()
            + "ie. listOf(1,2, true, 'string')";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return emptyList();
        }
        List<Object> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            Object object = arguments.get(i).eval(context);
            result.add(EvalUtils.unwrap(object));
        }
        return result;
    }
}
