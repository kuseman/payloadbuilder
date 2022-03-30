package org.kuse.payloadbuilder.core.catalog.system;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

/** Max input */
class MaxFunction extends ScalarFunctionInfo
{
    MaxFunction(Catalog catalog)
    {
        super(catalog, "max");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
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
