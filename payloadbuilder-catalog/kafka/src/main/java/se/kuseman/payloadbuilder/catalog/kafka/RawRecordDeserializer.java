package se.kuseman.payloadbuilder.catalog.kafka;

import java.nio.charset.StandardCharsets;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Returns raw bytes as UTF-8 strings */
class RawRecordDeserializer implements IRecordDeserializer
{
    @Override
    public Object deserializeKey(byte[] keyBytes)
    {
        if (keyBytes == null)
        {
            return null;
        }
        return toText(keyBytes);
    }

    @Override
    public Object deserializeValue(byte[] valueBytes)
    {
        if (valueBytes == null)
        {
            return null;
        }
        return toText(valueBytes);
    }

    /** Decode bytes as a UTF-8 string */
    static UTF8String toText(byte[] bytes)
    {
        return UTF8String.from(new String(bytes, StandardCharsets.UTF_8));
    }
}
