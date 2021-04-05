package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;
import java.util.regex.Pattern;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Regexp like. Matches input with a regular expression */
class RegexpLikeFunction extends ScalarFunctionInfo
{
    RegexpLikeFunction(Catalog catalog)
    {
        super(catalog, "regexp_like");
    }

    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }

    @Override
    public String getDescription()
    {
        return "Matches first argument to regex provided in second argument." + System.lineSeparator()
            + "Ex. regexp_like(expression, stringExpression)." + System.lineSeparator()
            + "Returns a boolean value.";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);

        if (obj == null)
        {
            return null;
        }

        Object patternObj = arguments.get(1).eval(context);
        if (!(patternObj instanceof String))
        {
            throw new IllegalArgumentException("Expected a String pattern for function " + getName() + " but got " + patternObj);
        }

        String value = String.valueOf(obj);
        Pattern pattern = Pattern.compile((String) patternObj);

        return pattern.matcher(value).find();
    }
}
