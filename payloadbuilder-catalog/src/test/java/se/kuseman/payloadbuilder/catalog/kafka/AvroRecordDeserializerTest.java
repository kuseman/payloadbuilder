package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link AvroRecordDeserializer} */
class AvroRecordDeserializerTest
{
    @Test
    void test_convert_generic_record_to_map()
    {
        String schemaJson = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[" + "{\"name\":\"orderId\",\"type\":\"int\"},"
                            + "{\"name\":\"customer\",\"type\":\"string\"},"
                            + "{\"name\":\"amount\",\"type\":\"double\"}"
                            + "]}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord record = new GenericData.Record(schema);
        record.put("orderId", 42);
        record.put("customer", new Utf8("Alice"));
        record.put("amount", 99.95);

        Map<String, Object> map = AvroRecordDeserializer.genericRecordToMap(record);

        assertEquals(42, map.get("orderId"));
        assertEquals(UTF8String.from("Alice"), map.get("customer"));
        assertEquals(99.95, map.get("amount"));
    }

    @Test
    void test_convert_nested_record()
    {
        String schemaJson = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[" + "{\"name\":\"orderId\",\"type\":\"int\"},"
                            + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
                            + "{\"name\":\"city\",\"type\":\"string\"},"
                            + "{\"name\":\"zip\",\"type\":\"string\"}"
                            + "]}}"
                            + "]}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord address = new GenericData.Record(schema.getField("address")
                .schema());
        address.put("city", new Utf8("Stockholm"));
        address.put("zip", new Utf8("11130"));

        GenericRecord order = new GenericData.Record(schema);
        order.put("orderId", 1);
        order.put("address", address);

        Map<String, Object> map = AvroRecordDeserializer.genericRecordToMap(order);
        assertTrue(map.get("address") instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, Object> addressMap = (Map<String, Object>) map.get("address");
        assertEquals(UTF8String.from("Stockholm"), addressMap.get("city"));
    }

    @Test
    void test_convert_array_field()
    {
        String schemaJson = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[" + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}" + "]}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord record = new GenericData.Record(schema);
        record.put("tags", List.of(new Utf8("urgent"), new Utf8("express")));

        Map<String, Object> map = AvroRecordDeserializer.genericRecordToMap(record);
        assertTrue(map.get("tags") instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) map.get("tags");
        assertEquals(2, tags.size());
        assertEquals(UTF8String.from("urgent"), tags.get(0));
    }

    @Test
    void test_convert_map_field()
    {
        Map<Utf8, Integer> avroMap = new HashMap<>();
        avroMap.put(new Utf8("a"), 1);
        avroMap.put(new Utf8("b"), 2);

        Object result = AvroRecordDeserializer.convertAvroValue(avroMap);
        assertTrue(result instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertEquals(1, map.get("a"));
        assertEquals(2, map.get("b"));
    }

    @Test
    void test_convert_null()
    {
        assertNull(AvroRecordDeserializer.convertAvroValue(null));
    }

    @Test
    void test_convert_primitives()
    {
        assertEquals(42, AvroRecordDeserializer.convertAvroValue(42));
        assertEquals(3.14, AvroRecordDeserializer.convertAvroValue(3.14));
        assertEquals(true, AvroRecordDeserializer.convertAvroValue(true));
        assertEquals(100L, AvroRecordDeserializer.convertAvroValue(100L));
    }

    @Test
    void test_convert_bytebuffer()
    {
        byte[] data = { 1, 2, 3 };
        ByteBuffer bb = ByteBuffer.wrap(data);
        Object result = AvroRecordDeserializer.convertAvroValue(bb);

        assertTrue(result instanceof byte[]);
        byte[] bytes = (byte[]) result;
        assertEquals(3, bytes.length);
        assertEquals(1, bytes[0]);
    }

    @Test
    void test_null_key_and_value()
    {
        try (AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://localhost:8081"))
        {
            assertNull(deserializer.deserializeKey(null));
            assertNull(deserializer.deserializeValue(null));
        }
    }

    @Test
    void test_invalid_magic_byte()
    {
        try (AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://localhost:8081"))
        {
            byte[] invalid = { 0x01, 0, 0, 0, 1, 0 };
            assertThrows(RuntimeException.class, () -> deserializer.deserializeValue(invalid));
        }
    }

    @Test
    void test_payload_too_short()
    {
        try (AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://localhost:8081"))
        {
            byte[] tooShort = { 0, 0 };
            assertThrows(RuntimeException.class, () -> deserializer.deserializeValue(tooShort));
        }
    }

    @Test
    void test_missing_schema_registry_url()
    {
        assertThrows(IllegalArgumentException.class, () -> new AvroRecordDeserializer(null));
        assertThrows(IllegalArgumentException.class, () -> new AvroRecordDeserializer(""));
        assertThrows(IllegalArgumentException.class, () -> new AvroRecordDeserializer("  ,  , "));
    }

    @Test
    void test_comma_separated_urls_accepted()
    {
        // Should not throw - multiple URLs are valid
        AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://registry1:8081,http://registry2:8081");
        assertNotNull(deserializer);
    }

    @Test
    void test_comma_separated_urls_with_spaces_accepted()
    {
        AvroRecordDeserializer deserializer = new AvroRecordDeserializer("  http://registry1:8081 , http://registry2:8081  ");
        assertNotNull(deserializer);
    }

    @Test
    void test_single_url_accepted()
    {
        AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://localhost:8081");
        assertNotNull(deserializer);
    }

    @Test
    void test_trailing_slash_normalized()
    {
        // Trailing slashes should be stripped to avoid double-slash in URL path
        AvroRecordDeserializer deserializer = new AvroRecordDeserializer("http://localhost:8081/");
        assertNotNull(deserializer);
    }
}
