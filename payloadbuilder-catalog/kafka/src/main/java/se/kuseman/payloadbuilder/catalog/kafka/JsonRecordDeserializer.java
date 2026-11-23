package se.kuseman.payloadbuilder.catalog.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.JsonParser;
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

    /**
     * Parse {@code length} bytes from {@code offset} as one JSON value that must span the whole range (only whitespace may follow it). Returns the same objects as {@link #deserializeValue(byte[])}.
     *
     * @throws IOException if the bytes aren't JSON or have content after the value
     */
    static Object parseStrict(byte[] bytes, int offset, int length) throws IOException
    {
        try (JsonParser parser = MAPPER.createParser(bytes, offset, length))
        {
            Object value = MAPPER.readValue(parser, Object.class);
            if (parser.nextToken() != null)
            {
                throw new IOException("Unexpected content after the JSON value at " + parser.currentLocation());
            }
            return value;
        }
    }
}
