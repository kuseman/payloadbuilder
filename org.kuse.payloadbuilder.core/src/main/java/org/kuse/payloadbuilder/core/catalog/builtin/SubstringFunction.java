package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Substring function */
class SubstringFunction extends ScalarFunctionInfo
{
    SubstringFunction(Catalog catalog)
    {
        super(catalog, "substring");
    }

    @Override
    public String getDescription()
    {
        return "Returns part of a string according to provided arguments." + System.lineSeparator()
            + "Ex. substring(<expression>, <start expression> [, <length expression>] ) ";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj == null)
        {
            return null;
        }

        String value = String.valueOf(obj);
        obj = arguments.get(1).eval(context);
        if (obj == null)
        {
            return null;
        }
        if (!(obj instanceof Integer))
        {
            throw new IllegalArgumentException("Expected integer argument for start to function " + getName() + " but got: " + obj);
        }

        int start = (Integer) obj;

        if (arguments.size() > 2)
        {
            obj = arguments.get(2).eval(context);
            if (obj == null)
            {
                return null;
            }
            if (!(obj instanceof Integer))
            {
                throw new IllegalArgumentException("Expected integer argument for length to function " + getName() + " but got: " + obj);
            }

            return value.substring(start, start + (int) obj);
        }

        return value.substring(start);
    }
}
