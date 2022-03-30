package org.kuse.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

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
            + "Ex. isblank(expression1, expression2)" + System.lineSeparator()
            + "If both arguments is blank, second argument is returned. " + System.lineSeparator()
            + "NOTE! First argument is transfomed to a string to determine blank-ness.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj != null && !isBlank(String.valueOf(obj)))
        {
            return obj;
        }

        return arguments.get(1).eval(context);
    }
}
