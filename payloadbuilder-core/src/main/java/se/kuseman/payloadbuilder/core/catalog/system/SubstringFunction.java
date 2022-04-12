package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

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
        return "Returns part of a string according to provided arguments." + System.lineSeparator() + "Ex. substring(<expression>, <start expression> [, <length expression>] ) ";
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        if (obj == null)
        {
            return null;
        }

        String value = String.valueOf(obj);
        obj = arguments.get(1)
                .eval(context);
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
            obj = arguments.get(2)
                    .eval(context);
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
