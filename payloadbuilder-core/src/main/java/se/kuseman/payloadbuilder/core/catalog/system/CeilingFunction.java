package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Math ceiling function */
class CeilingFunction extends AUnaryMathFunction
{
    public CeilingFunction()
    {
        super("ceiling");
    }

    @Override
    protected int getValue(int value)
    {
        return value;
    }

    @Override
    protected long getValue(long value)
    {
        return value;
    }

    @Override
    protected float getValue(float value)
    {
        return (float) Math.ceil(value);
    }

    @Override
    protected double getValue(double value)
    {
        return Math.ceil(value);
    }

    @Override
    protected Decimal getValue(Decimal value)
    {
        return value.ceiling();
    }

    @Override
    protected Object getValue(Object value)
    {
        return ExpressionMath.ceiling(value);
    }
}
