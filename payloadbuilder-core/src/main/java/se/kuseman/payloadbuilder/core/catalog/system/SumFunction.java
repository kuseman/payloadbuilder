package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Sums input */
class SumFunction extends AggregateFunction
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum");
    }

    @Override
    Object getAggregatorConstantValue(Object value, List<Tuple> tuples)
    {
        // Sum of a constant is the constant multiplied by the rows in the group
        return ExpressionMath.multiply(value, tuples.size());
    }

    @Override
    BiFunction<Object, Object, Object> getAggregatorAccumulator()
    {
        /*
         * @formatter:off
         * According to ANSI/TSQL SQL there are some variants of SUM
         * - SUM(<expr>)                 => Sum of ALL non null values of <expr> for the group's rows
         *
         * - SUM(ALL | DISTINCT <expr>)  => ALL is the default so that is the same as SUM(<expr>)
         *                                    DISTINCT picks the sum of all the distinct values in the group by evaluating the expression.
         *                                    NOT supported since SUM only takes one argument
         *                                    Might add a SUM_DISTINCT function instead
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

                return ExpressionMath.add(prev, next);
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    Object evalNonAggregated(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        Object sum = null;
        if (obj instanceof Iterator)
        {
            sum = sum((Iterator<Object>) obj);
        }
        else if (obj instanceof Iterable)
        {
            sum = sum(((Iterable<Object>) obj).iterator());
        }
        else
        {
            return obj;
        }
        return sum;
    }

    private Object sum(Iterator<Object> it)
    {
        Object sum = null;
        while (it.hasNext())
        {
            if (sum == null)
            {
                sum = it.next();
            }
            else
            {
                sum = ExpressionMath.add(sum, it.next());
            }
        }
        return sum;
    }
}
