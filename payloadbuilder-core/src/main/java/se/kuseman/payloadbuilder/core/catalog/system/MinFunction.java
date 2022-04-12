package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Min input */
class MinFunction extends ScalarFunctionInfo
{
    MinFunction(Catalog catalog)
    {
        super(catalog, "min");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
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
