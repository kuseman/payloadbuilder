package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Sums input */
class SumFunction extends ScalarFunctionInfo
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
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
