package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Lower and upper function */
class TrimFunction extends ScalarFunctionInfo
{
    private final Type type;

    TrimFunction(Type type)
    {
        super(type.name, FunctionType.SCALAR);
        this.type = type;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + type.descriptiveName + " string value of provided argument." + System.lineSeparator() + "If argument is non String the argument is returned as is.";
    }

    @Override
    public Arity arity()
    {
        return new Arity(1, 2);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, final TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

        final ValueVector trimChars = arguments.size() > 1 ? arguments.get(1)
                .eval(input, context)
                : null;
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Column.Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public UTF8String getString(int row)
            {
                String strArg = value.getString(row)
                        .toString();
                String stripChars = trimChars != null ? trimChars.getString(row)
                        .toString()
                        : null;
                switch (type)
                {
                    case BOTH:
                        return UTF8String.from(StringUtils.strip(strArg, stripChars));
                    case LEFT:
                        return UTF8String.from(StringUtils.stripStart(strArg, stripChars));
                    case RIGHT:
                        return UTF8String.from(StringUtils.stripEnd(strArg, stripChars));
                    default:
                        throw new IllegalArgumentException("Unsupported trim type: " + type);
                }
            }
        };
    }

    /** Type */
    enum Type
    {
        BOTH("trim", "trimmed"),
        LEFT("ltrim", "left trimmed"),
        RIGHT("rtrim", "right trimmed");

        final String name;
        final String descriptiveName;

        Type(String name, String descriptiveName)
        {
            this.name = name;
            this.descriptiveName = descriptiveName;
        }
    }
}
