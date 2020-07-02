package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** Counts input */
class CountFunction extends ScalarFunctionInfo
{
    CountFunction(Catalog catalog)
    {
        super(catalog, "count", Type.SCALAR);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        int count = 0;
        if (obj instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) obj;
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
        else
        {
            // Everything else is 1
            count = 1;
        }
        return count;
    }
}
