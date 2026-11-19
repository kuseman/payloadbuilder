package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Test of {@link JsonRecordDeserializer} */
class JsonRecordDeserializerTest
{
    private final JsonRecordDeserializer deserializer = new JsonRecordDeserializer();

    @Test
    void test_simple_json_object()
    {
        byte[] json = "{\"name\":\"Alice\",\"age\":30}".getBytes(StandardCharsets.UTF_8);
        Object result = deserializer.deserializeValue(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertEquals("Alice", map.get("name"));
        assertEquals(30, map.get("age"));
    }

    @Test
    void test_nested_json()
    {
        byte[] json = "{\"order\":{\"id\":1,\"items\":[\"a\",\"b\"]}}".getBytes(StandardCharsets.UTF_8);
        Object result = deserializer.deserializeValue(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertTrue(map.get("order") instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, Object> order = (Map<String, Object>) map.get("order");
        assertEquals(1, order.get("id"));
        assertTrue(order.get("items") instanceof List);
    }

    @Test
    void test_null_value()
    {
        assertNull(deserializer.deserializeValue(null));
    }

    @Test
    void test_invalid_json()
    {
        byte[] invalid = "not json".getBytes(StandardCharsets.UTF_8);
        assertThrows(RuntimeException.class, () -> deserializer.deserializeValue(invalid));
    }

    @Test
    void test_key_is_string()
    {
        Object result = deserializer.deserializeKey("my-key".getBytes(StandardCharsets.UTF_8));
        assertEquals("my-key", result.toString());
    }

    @Test
    void test_null_key()
    {
        assertNull(deserializer.deserializeKey(null));
    }
}
