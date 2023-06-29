package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Regexp like. Matches input with a regular expression */
class RegexpLikeFunction extends ScalarFunctionInfo
{
    RegexpLikeFunction()
    {
        super("regexp_like", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.Boolean);
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
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final ValueVector pattern = arguments.get(1)
                .eval(input, context);
        final Pattern constantPattern = arguments.get(1)
                .isConstant()
                        ? Pattern.compile(pattern.getString(0)
                                .toString())
                        : null;
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Boolean);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || pattern.isNull(row);
            }

            @Override
            public boolean getBoolean(int row)
            {
                String strValue = value.getString(row)
                        .toString();
                Pattern regexPattern;
                if (constantPattern != null)
                {
                    regexPattern = constantPattern;
                }
                else
                {
                    regexPattern = Pattern.compile(pattern.getString(row)
                            .toString());
                }

                return regexPattern.matcher(strValue)
                        .find();
            }
        };
    }
}
