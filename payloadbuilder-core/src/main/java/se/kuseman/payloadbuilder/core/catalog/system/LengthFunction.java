package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Length of string function */
class LengthFunction extends ScalarFunctionInfo
{
    LengthFunction(Catalog catalog)
    {
        super(catalog, "length");
    }

    @Override
    public String getDescription()
    {
        return "Returns length of provided arguemtn in string form." + System.lineSeparator() + "Ex. length(<expression>) " + System.lineSeparator() + "NOTE! First argument is converted to a string.";
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
        return value.length();
    }
}
