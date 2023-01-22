package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Sum aggregate. Sum all non null inputs */
class AggregateSumFunction extends ANumericAggregateFunction
{
    AggregateSumFunction()
    {
        super("sum");
    }

    @Override
    protected IntAggregator createIntAggregator()
    {
        return (a, b) -> Math.addExact(a, b);
    }

    @Override
    protected LongAggregator createLongAggregator()
    {
        return (a, b) -> Math.addExact(a, b);
    }

    @Override
    protected DecimalAggregator createDecimalAggregator()
    {
        return new DecimalAggregator()
        {
            @Override
            public Decimal aggregate(Decimal current, Decimal next)
            {
                if (current == null)
                {
                    return next;
                }
                return current.processArithmetic(next, IArithmeticBinaryExpression.Type.ADD);
            }
        };
    }

    @Override
    protected FloatAggregator createFloatAggregator()
    {
        return (a, b) -> a + b;
    }

    @Override
    protected DoubleAggregator createDoubleAggregator()
    {
        return (a, b) -> a + b;
    }

    @Override
    protected ObjectAggregator createObjectAggregator()
    {
        return new ObjectAggregator()
        {
            @Override
            public Object aggregate(Object current, Object next)
            {
                if (current == null)
                {
                    return next;
                }
                return ExpressionMath.add(current, next);
            }
        };
    }

    @Override
    public String toString()
    {
        return "SUM";
    }
}
