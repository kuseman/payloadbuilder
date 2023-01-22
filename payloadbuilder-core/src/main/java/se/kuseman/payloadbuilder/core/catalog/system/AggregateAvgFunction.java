package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Avg aggregate. Return the avg value among values */
class AggregateAvgFunction extends ANumericAggregateFunction
{
    AggregateAvgFunction(Catalog catalog)
    {
        super(catalog, "avg");
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
