package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Max input */
class MaxFunction extends AggregateFunction
{
    MaxFunction(Catalog catalog)
    {
        super(catalog, "max");
    }

    @Override
    Object getAggregatorConstantValue(Object value, List<Tuple> tuples)
    {
        // Max of a constant is the constant
        return value;
    }

    @Override
    BiFunction<Object, Object, Object> getAggregatorAccumulator()
    {
        /*
         * @formatter:off
         * According to ANSI/TSQL SQL there are some variants of MAX
         * - MAX(<expr>)                 => Max of ALL non null values of <expr> for the group's rows
         *
         * - MAX(ALL | DISTINCT <expr>)  => ALL is the default so that is the same as MAX(<expr>)
         *                                    DISTINCT picks the max of all the distinct values in the group by evaluating the expression.
         *                                    NOT supported since MAX only takes one argument
         *                                    Might add a MAX_DISTINCT function instead
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
                return cmp < 0 ? next
                        : prev;
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    Object evalNonAggregated(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        Object max = null;
        if (obj instanceof Iterator)
        {
            max = max((Iterator<Object>) obj);
        }
        else if (obj instanceof Iterable)
        {
            max = max(((Iterable<Object>) obj).iterator());
        }
        else
        {
            return obj;
        }
        return max;
    }

    private Object max(Iterator<Object> it)
    {
        Object max = null;
        while (it.hasNext())
        {
            if (max == null)
            {
                max = it.next();
            }
            else
            {
                Object val = it.next();
                int r = ExpressionMath.cmp(max, val);
                if (r < 0)
                {
                    max = val;
                }
            }
        }
        return max;
    }
}
