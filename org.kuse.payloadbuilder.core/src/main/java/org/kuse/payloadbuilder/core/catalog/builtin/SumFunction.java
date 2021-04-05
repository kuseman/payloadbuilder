package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

/** Sums input */
class SumFunction extends ScalarFunctionInfo
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
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
