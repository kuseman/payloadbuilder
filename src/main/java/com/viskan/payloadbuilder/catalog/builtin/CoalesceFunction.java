package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import java.util.List;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction(Catalog catalog)
    {
        super(catalog, "coalesce", Type.SCALAR);
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        for (Expression arg : arguments)
        {
            Object obj = arg.eval(context, row);
            if (obj != null)
            {
                return obj;
            }
        }
        
        return null;
    }
}
