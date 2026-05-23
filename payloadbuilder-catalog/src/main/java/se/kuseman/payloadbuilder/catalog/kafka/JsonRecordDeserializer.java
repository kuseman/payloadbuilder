package se.kuseman.payloadbuilder.catalog.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Deserializes JSON values into java Object for Type.Any access */
class JsonRecordDeserializer implements IRecordDeserializer
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Object deserializeKey(byte[] keyBytes)
    {
        if (keyBytes == null)
        {
            return null;
        }
        return UTF8String.from(new String(keyBytes, StandardCharsets.UTF_8));
    }

    @Override
    public Object deserializeValue(byte[] valueBytes)
    {
        if (valueBytes == null)
        {
            return null;
        }
        try
        {
            return MAPPER.readValue(valueBytes, Object.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to deserialize JSON value", e);
        }
    }
}
