package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction(Catalog catalog)
    {
        super(catalog, "isnull", Type.SCALAR);
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj != null)
        {
            return obj;
        }
        
        return arguments.get(1).eval(context);
    }
}
