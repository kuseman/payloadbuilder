package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Lower and upper function */
class LowerUpperFunction extends ScalarFunctionInfo
{
    private final boolean lower;

    LowerUpperFunction(Catalog catalog, boolean lower)
    {
        super(catalog, lower ? "lower"
                : "upper");
        this.lower = lower;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (lower ? "lower"
                : "upper")
               + " case of provided argument."
               + System.lineSeparator()
               + "NOTE! Argument is converted to a string.";
    }

    @Override
    public int arity()
    {
        return 1;
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
        return lower ? value.toLowerCase()
                : value.toUpperCase();
    }
}
