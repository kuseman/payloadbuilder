package se.kuseman.payloadbuilder.core.catalog.system;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Turns a json string into object */
class JsonValueFunction extends ScalarFunctionInfo
{
    private static final ObjectReader READER = new ObjectMapper().readerFor(Object.class);

    JsonValueFunction(Catalog catalog)
    {
        super(catalog, "json_value");
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object arg = arguments.get(0)
                .eval(context);
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
