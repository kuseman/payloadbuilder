package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.catalog.kafka.SchemaRegistryClient.RegisteredSchema;

/** Test of {@link AutoRecordDeserializer} */
class AutoRecordDeserializerTest
{
    private static final IntFunction<RegisteredSchema> NO_REGISTRY = null;

    private final JsonRecordDeserializer json = new JsonRecordDeserializer();

    @Test
    void test_null()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertNull(d.deserializeValue(null));
        assertNull(d.deserializeKey(null));
    }

    @Test
    void test_key_is_text()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertEquals(UTF8String.from("{\"a\":1}"), d.deserializeKey(bytes("{\"a\":1}")));
    }

    @Test
    void test_json_values_match_json_deserializer()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        for (String value : List.of("{\"name\":\"Alice\",\"age\":30}", "[1,2,\"x\"]", "42", "-1.5", "1e5", "2.5E-3", " 7 \n", "true", "false", " true\r\n", "\"quoted\""))
        {
            Object expected = json.deserializeValue(bytes(value));
            assertEquals(expected, d.deserializeValue(bytes(value)), value);
        }
        assertEquals(42, d.deserializeValue(bytes("42")));
        assertEquals(true, d.deserializeValue(bytes("true")));
        assertEquals("quoted", d.deserializeValue(bytes("\"quoted\"")));
        assertTrue(d.deserializeValue(bytes("{\"a\":1}")) instanceof Map);
        assertTrue(d.deserializeValue(bytes("[1]")) instanceof List);
    }

    @Test
    void test_json_null_literal()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertNull(d.deserializeValue(bytes("null")));
        assertNull(d.deserializeValue(bytes("  null \n")));
    }

    @Test
    void test_leading_whitespace_and_bom()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertEquals(Map.of("a", 1), d.deserializeValue(bytes(" \t\r\n{\"a\":1}  \n")));

        byte[] withBom = concat(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }, bytes("{\"a\":1}"));
        assertEquals(Map.of("a", 1), d.deserializeValue(withBom));

        byte[] withBomAndWhitespace = concat(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }, bytes("  [1]"));
        assertEquals(List.of(1), d.deserializeValue(withBomAndWhitespace));
    }

    @Test
    void test_text()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        for (String value : List.of("Customer 0", "{not json", "42 apples", "true story", "nothing", "[1,2", "\"unterminated", "{\"a\":1} trailing", "åäö", "2026-11-22 INFO start", "-x", "1.2.3",
                "12 34", "nullable", "falsey", "truex", "null x", "{\"a\":1}{\"a\":2}"))
        {
            assertEquals(UTF8String.from(value), d.deserializeValue(bytes(value)), value);
        }
    }

    @Test
    void test_empty_value_is_empty_string()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertEquals(UTF8String.from(""), d.deserializeValue(new byte[0]));
        assertEquals(UTF8String.from("   "), d.deserializeValue(bytes("   ")));
    }

    @Test
    void test_short_value_starting_with_nul_is_text()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> fail("lookup"), null);
        byte[] value = { 0, 'a', 'b' };
        assertEquals(UTF8String.from(new String(value, StandardCharsets.UTF_8)), d.deserializeValue(value));
    }

    @Test
    void test_no_registry_wire_format_json_payload()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        assertEquals(Map.of("a", 1), d.deserializeValue(wire(7, bytes("{\"a\":1}"))));
    }

    @Test
    void test_no_registry_wire_format_other_payload_is_text()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(NO_REGISTRY, null);
        byte[] value = wire(7, new byte[] { 2, 10, 'A', 'l', 'i', 'c', 'e' });
        assertEquals(UTF8String.from(new String(value, StandardCharsets.UTF_8)), d.deserializeValue(value));

        byte[] notJson = wire(7, bytes("{not json"));
        assertEquals(UTF8String.from(new String(notJson, StandardCharsets.UTF_8)), d.deserializeValue(notJson));
    }

    @Test
    void test_registry_avro_goes_to_delegate()
    {
        List<byte[]> received = new ArrayList<>();
        Object decoded = Map.of("decoded", true);
        IRecordDeserializer delegate = new StubDeserializer(received, decoded);
        AtomicInteger created = new AtomicInteger();

        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "AVRO"), () ->
        {
            created.incrementAndGet();
            return delegate;
        });

        byte[] value = wire(5, new byte[] { 2, 4 });
        assertSame(decoded, d.deserializeValue(value));
        assertSame(decoded, d.deserializeValue(value));
        assertEquals(1, created.get());
        assertEquals(2, received.size());
        assertSame(value, received.get(0));
    }

    @Test
    void test_registry_schema_id_is_read_big_endian()
    {
        List<Integer> ids = new ArrayList<>();
        AutoRecordDeserializer d = new AutoRecordDeserializer(id ->
        {
            ids.add(id);
            return schema(id, "JSON");
        }, null);

        d.deserializeValue(wire(0x01020304, bytes("1")));
        d.deserializeValue(wire(0xFFFFFFFE, bytes("1")));
        assertEquals(List.of(0x01020304, 0xFFFFFFFE), ids);
    }

    @Test
    void test_registry_json_schema()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "JSON"), () -> fail("avro factory"));
        assertEquals(Map.of("a", 1), d.deserializeValue(wire(3, bytes("{\"a\":1}"))));
        assertEquals(List.of(1, 2), d.deserializeValue(wire(3, bytes(" [1,2] "))));
    }

    @Test
    void test_registry_json_schema_invalid_payload_fails()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "JSON"), null);
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(3, bytes("{not json"))));
        assertEquals("Failed to deserialize JSON Schema value (schema id 3)", e.getMessage());

        e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(3, bytes("{\"a\":1} x"))));
        assertEquals("Failed to deserialize JSON Schema value (schema id 3)", e.getMessage());
    }

    @Test
    void test_registry_protobuf_fails()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "PROTOBUF"), () -> fail("avro factory"));
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(9, new byte[] { 0, 8, 1 })));
        assertEquals("Value is a Protobuf schema-registry value (schema id 9); Protobuf isn't supported", e.getMessage());
    }

    @Test
    void test_registry_unknown_schema_type_fails()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "XML"), null);
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(9, bytes("<a/>"))));
        assertEquals("Value has unsupported schema type 'XML' (schema id 9)", e.getMessage());
    }

    @Test
    void test_registry_lookup_failure_fails_value()
    {
        RuntimeException lookupError = new RuntimeException("Failed to fetch schema id 9");
        AutoRecordDeserializer d = new AutoRecordDeserializer(id ->
        {
            throw lookupError;
        }, null);
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(9, bytes("{\"a\":1}"))));
        assertSame(lookupError, e);
    }

    @Test
    void test_registry_does_not_affect_values_without_header()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> fail("lookup"), () -> fail("avro factory"));
        assertEquals(Map.of("a", 1), d.deserializeValue(bytes("{\"a\":1}")));
        assertEquals(UTF8String.from("Customer 0"), d.deserializeValue(bytes("Customer 0")));
    }

    @Test
    void test_avro_library_missing()
    {
        AtomicInteger factoryCalls = new AtomicInteger();
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, id == 1 ? "AVRO"
                : "JSON"), () ->
                {
                    factoryCalls.incrementAndGet();
                    throw new NoClassDefFoundError("org/apache/avro/Schema");
                });

        String expected = "Value is in Avro schema-registry format (schema id 1) but the avro library isn't on the classpath";
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(1, new byte[] { 2 })));
        assertEquals(expected, e.getMessage());
        assertTrue(e.getCause() instanceof NoClassDefFoundError);

        e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(1, new byte[] { 4 })));
        assertEquals(expected, e.getMessage());
        assertEquals(1, factoryCalls.get());

        // JSON Schema values don't need avro
        assertEquals(Map.of("a", 1), d.deserializeValue(wire(2, bytes("{\"a\":1}"))));
        d.close();
    }

    @Test
    void test_avro_library_missing_on_use()
    {
        AtomicInteger factoryCalls = new AtomicInteger();
        IRecordDeserializer broken = new IRecordDeserializer()
        {
            @Override
            public Object deserializeKey(byte[] keyBytes)
            {
                return null;
            }

            @Override
            public Object deserializeValue(byte[] valueBytes)
            {
                throw new NoClassDefFoundError("org/apache/avro/generic/GenericDatumReader");
            }
        };
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "AVRO"), () ->
        {
            factoryCalls.incrementAndGet();
            return broken;
        });

        assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(1, new byte[] { 2 })));
        RuntimeException e = assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(1, new byte[] { 2 })));
        assertEquals("Value is in Avro schema-registry format (schema id 1) but the avro library isn't on the classpath", e.getMessage());
        assertEquals(1, factoryCalls.get());
    }

    @Test
    void test_avro_delegate_error_is_unchanged()
    {
        RuntimeException decodeError = new RuntimeException("Failed to decode Avro payload with schema id 1");
        IRecordDeserializer failing = new IRecordDeserializer()
        {
            @Override
            public Object deserializeKey(byte[] keyBytes)
            {
                return null;
            }

            @Override
            public Object deserializeValue(byte[] valueBytes)
            {
                throw decodeError;
            }
        };
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "AVRO"), () -> failing);
        assertSame(decodeError, assertThrows(RuntimeException.class, () -> d.deserializeValue(wire(1, new byte[] { 2 }))));
    }

    @Test
    void test_close_closes_created_delegate()
    {
        StubDeserializer delegate = new StubDeserializer(new ArrayList<>(), "x");
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "AVRO"), () -> delegate);
        d.deserializeValue(wire(1, new byte[] { 2 }));
        d.close();
        assertTrue(delegate.closed.get());
    }

    @Test
    void test_close_does_not_create_delegate()
    {
        AutoRecordDeserializer d = new AutoRecordDeserializer(id -> schema(id, "AVRO"), () -> fail("avro factory"));
        d.close();
    }

    @Test
    void test_string_constructor_without_registry()
    {
        try (AutoRecordDeserializer d = new AutoRecordDeserializer((String) null))
        {
            assertEquals(Map.of("a", 1), d.deserializeValue(wire(1, bytes("{\"a\":1}"))));
        }
        try (AutoRecordDeserializer d = new AutoRecordDeserializer("  "))
        {
            assertEquals(UTF8String.from("text"), d.deserializeValue(bytes("text")));
        }
    }

    private static <T> T fail(String what)
    {
        throw new AssertionError(what + " should not be called");
    }

    private static RegisteredSchema schema(int id, String type)
    {
        return new RegisteredSchema(id, type, "{}");
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] wire(int schemaId, byte[] payload)
    {
        return ByteBuffer.allocate(5 + payload.length)
                .put((byte) 0)
                .putInt(schemaId)
                .put(payload)
                .array();
    }

    private static byte[] concat(byte[] a, byte[] b)
    {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static class StubDeserializer implements IRecordDeserializer
    {
        private final List<byte[]> received;
        private final Object result;
        private final AtomicBoolean closed = new AtomicBoolean();

        StubDeserializer(List<byte[]> received, Object result)
        {
            this.received = received;
            this.result = result;
        }

        @Override
        public Object deserializeKey(byte[] keyBytes)
        {
            return null;
        }

        @Override
        public Object deserializeValue(byte[] valueBytes)
        {
            received.add(valueBytes);
            return result;
        }

        @Override
        public void close()
        {
            assertFalse(closed.getAndSet(true), "closed twice");
        }
    }
}
