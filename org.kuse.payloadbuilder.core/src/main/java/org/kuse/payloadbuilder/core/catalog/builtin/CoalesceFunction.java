package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction(Catalog catalog)
    {
        super(catalog, "coalesce", Type.SCALAR);
    }
    
    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator() +
                "Ex. coalesce(expression1, expression2, expression3, ...)"  + System.lineSeparator() +
                "If all arguments yield null, null is returned.";
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
