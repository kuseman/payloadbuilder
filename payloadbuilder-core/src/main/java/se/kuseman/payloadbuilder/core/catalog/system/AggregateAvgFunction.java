package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Avg aggregate. Return the avg value among values */
class AggregateAvgFunction extends ANumericAggregateFunction
{
    AggregateAvgFunction()
    {
        super("avg");
    }

    @Override
    protected IntAggregator createIntAggregator()
    {
        return new IntAggregator()
        {
            private int count = 0;

            @Override
            public int aggregate(int current, int next)
            {
                count++;
                return current + next;
            }

            @Override
            public int combine(int result)
            {
                return count == 0 ? result
                        : (result / count);
            }
        };
    }

    @Override
    protected LongAggregator createLongAggregator()
    {
        return new LongAggregator()
        {
            private int count = 0;

            @Override
            public long aggregate(long current, long next)
            {
                count++;
                return current + next;
            }

            @Override
            public long combine(long result)
            {
                return count == 0 ? result
                        : (result / count);
            }
        };
    }

    @Override
    protected DecimalAggregator createDecimalAggregator()
    {
        return new DecimalAggregator()
        {
            private int count = 0;

            @Override
            public Decimal aggregate(Decimal current, Decimal next)
            {
                count++;
                if (current == null)
                {
                    return next;
                }
                return current.processArithmetic(next, IArithmeticBinaryExpression.Type.ADD);
            }

            @Override
            public Decimal combine(Decimal result)
            {
                return count == 0 ? result
                        : result.processArithmetic(Decimal.from(count), IArithmeticBinaryExpression.Type.DIVIDE);
            }
        };
    }

    @Override
    protected FloatAggregator createFloatAggregator()
    {
        return new FloatAggregator()
        {
            private int count = 0;

            @Override
            public float aggregate(float current, float next)
            {
                count++;
                return current + next;
            }

            @Override
            public float combine(float result)
            {
                return count == 0 ? result
                        : (result / count);
            }
        };
    }

    @Override
    protected DoubleAggregator createDoubleAggregator()
    {
        return new DoubleAggregator()
        {
            private int count = 0;

            @Override
            public double aggregate(double current, double next)
            {
                count++;
                return current + next;
            }

            @Override
            public double combine(double result)
            {
                return count == 0 ? result
                        : (result / count);
            }
        };
    }

    @Override
    protected ObjectAggregator createObjectAggregator()
    {
        return new ObjectAggregator()
        {
            private int count = 0;

            @Override
            public Object aggregate(Object current, Object next)
            {
                count++;
                if (current == null)
                {
                    return next;
                }
                return ExpressionMath.add(current, next);
            }

            @Override
            public Object combine(Object result)
            {
                return count == 0 ? result
                        : ExpressionMath.divide(result, count);
            }
        };
    }

    @Override
    public String toString()
    {
        return "AVG";
    }
}
