package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Regexp match. Matches input with a regular expression and outputs pattern based on matching */
class RegexpMatchFunction extends ScalarFunctionInfo
{
    RegexpMatchFunction()
    {
        super("regexp_match", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        //@formatter:off
        return "Matches first argument to regex provided in second argument." + System.lineSeparator()
               + "Ex. regexp_match(expression, stringExpression)." + System.lineSeparator()
               + "This returns an array of values with matched gorups." + System.lineSeparator()
               + System.lineSeparator()
               + "Returns an array of values.";
        //@formatter:on
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.array(Type.String);
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
                return ResolvedType.array(Type.String);
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
            public ValueVector getArray(int row)
            {
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

                final Matcher matcher = regexPattern.matcher(value.getString(row)
                        .toString());

                final List<UTF8String> matches = new ArrayList<>();
                int groupCount = matcher.groupCount();
                while (matcher.find())
                {
                    for (int i = 1; i <= groupCount; i++)
                    {
                        matches.add(UTF8String.from(matcher.group(i)));
                    }
                }
                if (matches.isEmpty())
                {
                    // Empty vector
                    return ValueVector.literalString(UTF8String.EMPTY, 0);
                }

                return new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.String);
                    }

                    @Override
                    public int size()
                    {
                        return matches.size();
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return false;
                    }

                    @Override
                    public UTF8String getString(int row)
                    {
                        return matches.get(row);
                    }
                };
            }
        };
    }
}
