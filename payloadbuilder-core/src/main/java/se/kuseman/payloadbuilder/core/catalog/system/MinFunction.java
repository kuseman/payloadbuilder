package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Min input */
class MinFunction extends AggregateFunction
{
    MinFunction(Catalog catalog)
    {
        super(catalog, "min");
    }

    @Override
    Object getAggregatorConstantValue(Object value, List<Tuple> tuples)
    {
        // Min of a constant is the constant
        return value;
    }

    @Override
    BiFunction<Object, Object, Object> getAggregatorAccumulator()
    {
        /*
         * @formatter:off
         * According to ANSI/TSQL SQL there are some variants of MIN
         * - MIN(<expr>)                 => Min of ALL non null values of <expr> for the group's rows
         *
         * - MIN(ALL | DISTINCT <expr>)  => ALL is the default so that is the same as MIN(<expr>)
         *                                    DISTINCT picks the min of all the distinct values in the group by evaluating the expression.
         *                                    NOT supported since MIN only takes one argument
         *                                    Might add a MIN_DISTINCT function instead
         *
         * @formatter:on
         */

        return new BiFunction<Object, Object, Object>()
        {
            @Override
            public Object apply(Object prev, Object next)
            {
                if (prev == null)
                {
                    return next;
                }
                else if (next == null)
                {
                    return prev;
                }

                int cmp = ExpressionMath.cmp(prev, next);
                return cmp < 0 ? prev
                        : next;
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    Object evalNonAggregated(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        Object min = null;
        if (obj instanceof Iterator)
        {
            min = min((Iterator<Object>) obj);
        }
        else if (obj instanceof Iterable)
        {
            min = min(((Iterable<Object>) obj).iterator());
        }
        else
        {
            return obj;
        }
        return min;
    }

    private Object min(Iterator<Object> it)
    {
        Object min = null;
        while (it.hasNext())
        {
            if (min == null)
            {
                min = it.next();
            }
            else
            {
                Object val = it.next();
                int r = ExpressionMath.cmp(min, val);
                if (r > 0)
                {
                    min = val;
                }
            }
        }
        return min;
    }
}
