package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.ExpressionMath;

import java.util.Iterator;
import java.util.List;

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
