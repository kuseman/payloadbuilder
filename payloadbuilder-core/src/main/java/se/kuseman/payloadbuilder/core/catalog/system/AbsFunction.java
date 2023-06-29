package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Math abs function */
class AbsFunction extends AUnaryMathFunction
{
    public AbsFunction()
    {
        super("abs");
    }

    @Override
    protected int getValue(int value)
    {
        return Math.abs(value);
    }

    @Override
    protected long getValue(long value)
    {
        return Math.abs(value);
    }

    @Override
    protected float getValue(float value)
    {
        return Math.abs(value);
    }

    @Override
    protected double getValue(double value)
    {
        return Math.abs(value);
    }

    @Override
    protected Decimal getValue(Decimal value)
    {
        return value.abs();
    }

    @Override
    protected Object getValue(Object value)
    {
        return ExpressionMath.abs(value);
    }
}
