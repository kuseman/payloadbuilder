package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleList;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Counts input */
class CountFunction extends AggregateFunction
{
    CountFunction(Catalog catalog)
    {
        super(catalog, "count");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    Object evalNonAggregated(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        int count = 0;
        if (obj instanceof TupleList)
        {
            return ((TupleList) obj).size();
        }
        else if (obj instanceof TupleIterator)
        {
            TupleIterator it = (TupleIterator) obj;
            while (it.hasNext())
            {
                it.next();
                count++;
            }
        }
        else if (obj instanceof Collection)
        {
            count = ((Collection<Object>) obj).size();
        }
        else if (obj instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) obj;
            while (it.hasNext())
            {
                it.next();
                count++;
            }
        }
        else if (obj instanceof Map)
        {
            count = ((Map) obj).size();
        }
        else if (obj != null)
        {
            // Everything else is 1
            count = 1;
        }
        return count;
    }

    @Override
    Object getAggregatorConstantValue(Object value, List<Tuple> tuples)
    {
        return tuples.size();
    }

    @Override
    protected Object getAggregatorIdentity()
    {
        return 0;
    }

    @Override
    BiFunction<Object, Object, Object> getAggregatorAccumulator()
    {
        /*
      * @formatter:off
      * According to ANSI/TSQL SQL there are some variants of COUNT
      * - COUNT(*)                      => Special case that simply counts all values in the group
      *                                    Since COUNT is a runtime function the asterisk is not a valid token
      *                                    so this variant is not supported yet
      * - COUNT(<expr>)                 => Count ALL non null values of <expr> for the group's rows
      *                                    So for example COUNT(1) will count the scalar value 1 for all rows => size of rows
      *
      * - COUNT(ALL | DISTINCT <expr>)  => ALL is the default so that is the same as COUNT(<expr>)
      *                                    DISTINCT counts all the distinct values in the group by evaluating the expression.
      *                                    This is also not supported since COUNT only takes one argument
      *                                    Might add a COUNT_DISTINCT function instead
      *
      * @formatter:on
      */

        return new BiFunction<Object, Object, Object>()
        {
            @Override
            public Object apply(Object prev, Object next)
            {
                return (Integer) prev + (next != null ? 1
                        : 0);
            }
        };
    }
}
