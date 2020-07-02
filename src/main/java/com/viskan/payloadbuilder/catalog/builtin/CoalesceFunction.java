package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import java.util.List;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction(Catalog catalog)
    {
        super(catalog, "coalesce", Type.SCALAR);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        for (Expression arg : arguments)
        {
            Object obj = arg.eval(context);
            if (obj != null)
            {
                return obj;
            }
        }
        
        return null;
    }
}
