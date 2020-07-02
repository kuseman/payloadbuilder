package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Arrays.asList;

import java.util.List;

/** Function that returns a session property */
class PropertyFunction extends ScalarFunctionInfo
{
    PropertyFunction(Catalog catalog)
    {
        super(catalog, "property", Type.SCALAR);
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object value = arguments.get(0).eval(context);
        return context.getSession().getProperty(String.valueOf(value));
    }
}
