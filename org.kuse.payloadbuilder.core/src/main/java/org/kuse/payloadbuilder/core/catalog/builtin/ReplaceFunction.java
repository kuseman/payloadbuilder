package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Replace */
class ReplaceFunction extends ScalarFunctionInfo
{
    ReplaceFunction(Catalog catalog)
    {
        super(catalog, "replace");
    }

    @Override
    public String getDescription()
    {
        return "Replaces all occurrences of specified string with a replacement string" + System.lineSeparator() +
            "Ex. replace(expression, searchExpression, replaceExpression)" + System.lineSeparator() +
            "NOTE! All input arguments is converted to String if not String already." + System.lineSeparator() +
            "      If any input evaluates to null, null is returned";
    }

    @Override
    public Class<?> getDataType()
    {
        return String.class;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg0 = arguments.get(0).eval(context);
        Object arg1 = arguments.get(1).eval(context);
        Object arg2 = arguments.get(2).eval(context);
        if (arg0 == null || arg1 == null || arg2 == null)
        {
            return null;
        }
        String value = String.valueOf(arg0);
        String search = String.valueOf(arg1);
        String replacement = String.valueOf(arg2);
        return value.replace(search, replacement);
    }
}
