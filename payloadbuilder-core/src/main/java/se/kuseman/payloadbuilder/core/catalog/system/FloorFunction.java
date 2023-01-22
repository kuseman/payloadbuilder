package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Math floor function */
class FloorFunction extends AUnaryMathFunction
{
    public FloorFunction()
    {
        super("floor");
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
        return (float) Math.floor(value);
    }

    @Override
    protected double getValue(double value)
    {
        return Math.floor(value);
    }

    @Override
    protected Decimal getValue(Decimal value)
    {
        return value.floor();
    }

    @Override
    protected Object getValue(Object value)
    {
        return ExpressionMath.floor(value);
    }
}
