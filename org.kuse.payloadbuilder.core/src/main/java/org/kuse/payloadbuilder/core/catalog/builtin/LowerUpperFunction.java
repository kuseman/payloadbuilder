package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Lower and upper function */
class LowerUpperFunction extends ScalarFunctionInfo
{
    private final boolean lower;

    LowerUpperFunction(Catalog catalog, boolean lower)
    {
        super(catalog, lower ? "lower" : "upper");
        this.lower = lower;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (lower ? "lower" : "upper") + " case of provided argument." + System.lineSeparator()
            + "NOTE! Argument is converted to a string.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
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
        return lower ? value.toLowerCase() : value.toUpperCase();
    }
}
