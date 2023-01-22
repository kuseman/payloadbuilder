package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Sum aggregate. Sum all non null inputs */
class AggregateSumFunction extends ANumericAggregateFunction
{
    AggregateSumFunction(Catalog catalog)
    {
        super(catalog, "sum");
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
