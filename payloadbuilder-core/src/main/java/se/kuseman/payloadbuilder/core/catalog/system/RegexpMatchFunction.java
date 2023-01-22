package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Regexp match. Matches input with a regular expression and outputs pattern based on matching */
class RegexpMatchFunction extends ScalarFunctionInfo
{
    RegexpMatchFunction(Catalog catalog)
    {
        super(catalog, "regexp_match", FunctionType.SCALAR);
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
    public int arity()
    {
        return 2;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.valueVector(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
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
                return ResolvedType.valueVector(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable()
                        || pattern.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || pattern.isNull(row);
            }

            @Override
            public Object getValue(int row)
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
                    return ValueVector.literalObject(ResolvedType.of(Type.String), "", 0);
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
                    public boolean isNullable()
                    {
                        return false;
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

                    @Override
                    public Object getValue(int row)
                    {
                        throw new IllegalArgumentException("getValue should not be called on typed vectors");
                    }
                };
            }
        };
    }
}
