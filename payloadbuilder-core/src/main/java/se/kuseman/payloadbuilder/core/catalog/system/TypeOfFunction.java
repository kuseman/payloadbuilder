package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Returns type of provided argument */
class TypeOfFunction extends ScalarFunctionInfo
{
    TypeOfFunction(Catalog catalog)
    {
        super(catalog, "typeof");
    }

    @Override
    public String getDescription()
    {
        return "Returns type string of provided argument. " + System.lineSeparator() + "Ex. typeof(expression)" + System.lineSeparator() + "Mainly used when debugging values.";
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
        return obj == null ? null
                : obj.getClass()
                        .getSimpleName();
    }
}
