package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Lower and upper function */
class LowerUpperFunction extends ScalarFunctionInfo
{
    private final boolean lower;

    LowerUpperFunction(Catalog catalog, boolean lower)
    {
        super(catalog, lower ? "lower"
                : "upper", FunctionType.SCALAR);
        this.lower = lower;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (lower ? "lower"
                : "upper")
               + " case of provided argument."
               + System.lineSeparator()
               + "NOTE! Argument is converted to a string.";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

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
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public UTF8String getString(int row)
            {
                String strValue = value.getString(row)
                        .toString();
                return UTF8String.from(lower ? strValue.toLowerCase()
                        : strValue.toUpperCase());
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
    //
    // @Override
    // public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // Object obj = arguments.get(0)
    // .eval(context);
    // if (obj == null)
    // {
    // return null;
    // }
    // String value = String.valueOf(obj);
    // return lower ? value.toLowerCase()
    // : value.toUpperCase();
    // }
}
