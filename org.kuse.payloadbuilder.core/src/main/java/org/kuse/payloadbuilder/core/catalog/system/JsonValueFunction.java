package org.kuse.payloadbuilder.core.catalog.system;

import java.io.IOException;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/** Turns a json string into object */
class JsonValueFunction extends ScalarFunctionInfo
{
    private static final ObjectReader READER = new ObjectMapper().readerFor(Object.class);

    JsonValueFunction(Catalog catalog)
    {
        super(catalog, "json_value");
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
            return READER.readValue(String.valueOf(arg));
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Error deserializing '" + arg + "'", e);
        }
    }
}
