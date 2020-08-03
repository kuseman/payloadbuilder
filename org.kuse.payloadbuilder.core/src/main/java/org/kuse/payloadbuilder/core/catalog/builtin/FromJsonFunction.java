package org.kuse.payloadbuilder.core.catalog.builtin;

import java.io.IOException;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Turns a json string into object */
class FromJsonFunction extends ScalarFunctionInfo
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    FromJsonFunction(Catalog catalog)
    {
        super(catalog, "from_json", Type.SCALAR);
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg = arguments.get(0).eval(context);
        if (arg == null)
        {
            return null;
        }
        
        try
        {
            return MAPPER.readValue(String.valueOf(arg), Object.class);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Error deserializing " + arg, e);
        }
    }
}
