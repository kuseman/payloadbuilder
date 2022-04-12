package se.kuseman.payloadbuilder.core.catalog.system;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Returns first item that is not a blank string */
class IsBlankFunction extends ScalarFunctionInfo
{
    IsBlankFunction(Catalog catalog)
    {
        super(catalog, "isblank");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non blank value of provided arguments. " + System.lineSeparator()
               + "Ex. isblank(expression1, expression2)"
               + System.lineSeparator()
               + "If both arguments is blank, second argument is returned. "
               + System.lineSeparator()
               + "NOTE! First argument is transfomed to a string to determine blank-ness.";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        if (obj != null
                && !isBlank(String.valueOf(obj)))
        {
            return obj;
        }

        return arguments.get(1)
                .eval(context);
    }
}
