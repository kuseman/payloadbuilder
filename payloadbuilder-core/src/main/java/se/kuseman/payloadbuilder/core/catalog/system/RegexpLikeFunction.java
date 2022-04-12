package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Regexp like. Matches input with a regular expression */
class RegexpLikeFunction extends ScalarFunctionInfo
{
    RegexpLikeFunction(Catalog catalog)
    {
        super(catalog, "regexp_like");
    }

    @Override
    public DataType getDataType(List<? extends IExpression> arguments)
    {
        return DataType.BOOLEAN;
    }

    @Override
    public String getDescription()
    {
        return "Matches first argument to regex provided in second argument." + System.lineSeparator()
               + "Ex. regexp_like(expression, stringExpression)."
               + System.lineSeparator()
               + "Returns a boolean value.";
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

        Object patternObj = arguments.get(1)
                .eval(context);
        if (!(patternObj instanceof String))
        {
            throw new IllegalArgumentException("Expected a String pattern for function " + getName() + " but got " + patternObj);
        }

        String value = String.valueOf(obj);
        Pattern pattern = Pattern.compile((String) patternObj);

        return pattern.matcher(value)
                .find();
    }
}
