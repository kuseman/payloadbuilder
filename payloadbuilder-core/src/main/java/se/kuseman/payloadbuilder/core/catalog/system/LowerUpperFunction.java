package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Lower and upper function */
class LowerUpperFunction extends ScalarFunctionInfo
{
    private final boolean lower;

    LowerUpperFunction(boolean lower)
    {
        super(lower ? "lower"
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
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
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
        };
    }
}
