package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Min aggregate. Return the max value among values */
class AggregateMaxFunction extends ANumericAggregateFunction
{
    AggregateMaxFunction(Catalog catalog)
    {
        super(catalog, "max");
    }

    @Override
    protected IntAggregator createIntAggregator()
    {
        return new IntAggregator()
        {
            @Override
            public int getIdentity()
            {
                return Integer.MIN_VALUE;
            }

            @Override
            public int aggregate(int current, int next)
            {
                return current > next ? current
                        : next;
            }
        };
    }

    @Override
    protected LongAggregator createLongAggregator()
    {
        return new LongAggregator()
        {
            @Override
            public long getIdentity()
            {
                return Long.MIN_VALUE;
            }

            @Override
            public long aggregate(long current, long next)
            {
                return current > next ? current
                        : next;
            }
        };
    }

    @Override
    protected FloatAggregator createFloatAggregator()
    {
        return new FloatAggregator()
        {
            @Override
            public float getIdentity()
            {
                return -Float.MAX_VALUE;
            }

            @Override
            public float aggregate(float current, float next)
            {
                return current > next ? current
                        : next;
            }
        };
    }

    @Override
    protected DoubleAggregator createDoubleAggregator()
    {
        return new DoubleAggregator()
        {
            @Override
            public double getIdentity()
            {
                return -Double.MAX_VALUE;
            }

            @Override
            public double aggregate(double current, double next)
            {
                return current > next ? current
                        : next;
            }
        };
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
                return ExpressionMath.cmp(current, next) > 0 ? current
                        : next;
            }
        };
    }

    @Override
    public String toString()
    {
        return "MAX";
    }
}
