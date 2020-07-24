package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionMath;

/** Sums input */
class SumFunction extends ScalarFunctionInfo
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum", Type.SCALAR);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        Object sum = null;
        if (obj instanceof Iterator)
        {
            @SuppressWarnings("unchecked")
            Iterator<Object> it = (Iterator<Object>) obj;
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
        }
        else
        {
            return obj;
        }
        return sum;
    }
}
