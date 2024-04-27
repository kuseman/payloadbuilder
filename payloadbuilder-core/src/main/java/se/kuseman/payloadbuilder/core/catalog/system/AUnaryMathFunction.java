package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Base class for unary math functions */
abstract class AUnaryMathFunction extends ScalarFunctionInfo
{
    AUnaryMathFunction(String name)
    {
        super(name, FunctionType.SCALAR);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        ResolvedType type = arguments.get(0)
                .getType();
        if (!(type.getType()
                .isNumber()
                || type.getType() == Type.Any))
        {
            throw new IllegalArgumentException(getName() + " requires a numeric argument or " + Type.Any);
        }
        return type;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        IExpression arg = arguments.get(0);
        ValueVector value = arguments.get(0)
                .eval(input, context);

        Type type = arg.getType()
                .getType();
        int rowCount = input.getRowCount();

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(arg.getType(), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            switch (type)
            {
                case Int:
                    resultVector.setInt(i, getValue(value.getInt(i)));
                    break;
                case Long:
                    resultVector.setLong(i, getValue(value.getLong(i)));
                    break;
                case Float:
                    resultVector.setFloat(i, getValue(value.getFloat(i)));
                    break;
                case Double:
                    resultVector.setDouble(i, getValue(value.getDouble(i)));
                    break;
                case Decimal:
                    resultVector.setDecimal(i, getValue(value.getDecimal(i)));
                    break;
                case Any:
                    resultVector.setAny(i, getValue(value.getAny(i)));
                    break;
                default:
                    throw new IllegalArgumentException("Function " + getName() + " does not support type: " + type);
            }
        }
        return resultVector;
    }

    protected abstract int getValue(int value);

    protected abstract long getValue(long value);

    protected abstract float getValue(float value);

    protected abstract double getValue(double value);

    protected abstract Decimal getValue(Decimal value);

    protected abstract Object getValue(Object value);
}
