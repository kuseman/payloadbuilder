package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns type of provided argument */
class TypeOfFunction extends ScalarFunctionInfo
{
    TypeOfFunction(Catalog catalog)
    {
        super(catalog, "typeof", Type.SCALAR);
    }
    
    @Override
    public String getDescription()
    {
        return "Returns type string of provided argument. " + System.lineSeparator() +
                "Ex. typeof(expression)"  + System.lineSeparator() +
                "Mainly used when debugging values.";
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        return obj == null ? null : obj.getClass().getSimpleName();
    }
}
