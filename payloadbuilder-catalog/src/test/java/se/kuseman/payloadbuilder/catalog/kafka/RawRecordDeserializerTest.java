package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link RawRecordDeserializer} */
class RawRecordDeserializerTest
{
    private final RawRecordDeserializer deserializer = new RawRecordDeserializer();

    @Test
    void test_deserialize_key()
    {
        Object result = deserializer.deserializeKey("my-key".getBytes(StandardCharsets.UTF_8));
        assertEquals(UTF8String.from("my-key"), result);
    }

    @Test
    void test_deserialize_value()
    {
        Object result = deserializer.deserializeValue("raw-value".getBytes(StandardCharsets.UTF_8));
        assertEquals(UTF8String.from("raw-value"), result);
    }

    @Test
    void test_null_key()
    {
        assertNull(deserializer.deserializeKey(null));
    }

    @Test
    void test_null_value()
    {
        assertNull(deserializer.deserializeValue(null));
    }

    @Test
    void test_empty_value()
    {
        Object result = deserializer.deserializeValue(new byte[0]);
        assertEquals(UTF8String.from(""), result);
    }
}
