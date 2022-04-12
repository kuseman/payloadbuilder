package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction(Catalog catalog)
    {
        super(catalog, "coalesce");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
               + "Ex. coalesce(expression1, expression2, expression3, ...)"
               + System.lineSeparator()
               + "If all arguments yield null, null is returned.";
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        for (IExpression arg : arguments)
        {
            Object obj = arg.eval(context);
            if (obj != null)
            {
                return obj;
            }
        }

        return null;
    }
}
