package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Lower and upper function */
class TrimFunction extends ScalarFunctionInfo
{
    private final Type type;

    TrimFunction(Catalog catalog, Type type)
    {
        super(catalog, type.name, FunctionType.SCALAR);
        this.type = type;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + type.descriptiveName + " string value of provided argument." + System.lineSeparator() + "If argument is non String the argument is returned as is.";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, final TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

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
                // Boxing here for now
                Object arg = value.valueAsObject(row);
                String strArg = String.valueOf(arg);
                switch (type)
                {
                    case BOTH:
                        return UTF8String.from(StringUtils.trim(strArg));
                    case LEFT:
                        return UTF8String.from(StringUtils.stripStart(strArg, null));
                    case RIGHT:
                        return UTF8String.from(StringUtils.stripEnd(strArg, null));
                    default:
                        throw new IllegalArgumentException("Unsupported trim type: " + type);
                }
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
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
