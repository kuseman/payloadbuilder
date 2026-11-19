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
        return UTF8String.from(new String(keyBytes, StandardCharsets.UTF_8));
    }

    @Override
    public Object deserializeValue(byte[] valueBytes)
    {
        if (valueBytes == null)
        {
            return null;
        }
        return UTF8String.from(new String(valueBytes, StandardCharsets.UTF_8));
    }
}
