package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;

import java.util.List;

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
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        Object obj = arguments.get(0).eval(context, row);
        if (obj != null)
        {
            return obj;
        }
        
        return arguments.get(1).eval(context, row);
    }
}
