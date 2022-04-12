package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Max input */
class MaxFunction extends ScalarFunctionInfo
{
    MaxFunction(Catalog catalog)
    {
        super(catalog, "max");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
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
