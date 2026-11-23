package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.catalog.kafka.SchemaRegistryClient.RegisteredSchema;

/** Test of {@link SchemaRegistryClient} */
class SchemaRegistryClientTest
{
    @Test
    void test_missing_urls()
    {
        assertThrows(IllegalArgumentException.class, () -> new SchemaRegistryClient(null));
        assertThrows(IllegalArgumentException.class, () -> new SchemaRegistryClient(""));
        assertThrows(IllegalArgumentException.class, () -> new SchemaRegistryClient("   "));
        assertThrows(IllegalArgumentException.class, () -> new SchemaRegistryClient("  ,  , "));
    }

    @Test
    void test_single_url()
    {
        assertEquals(List.of("http://localhost:8081"), new SchemaRegistryClient("http://localhost:8081").getUrls());
    }

    @Test
    void test_comma_separated_urls()
    {
        assertEquals(List.of("http://registry1:8081", "http://registry2:8081"), new SchemaRegistryClient("http://registry1:8081,http://registry2:8081").getUrls());
        assertEquals(List.of("http://registry1:8081", "http://registry2:8081"), new SchemaRegistryClient("  http://registry1:8081 , http://registry2:8081  ").getUrls());
    }

    @Test
    void test_trailing_slash_normalized()
    {
        assertEquals(List.of("http://localhost:8081"), new SchemaRegistryClient("http://localhost:8081/").getUrls());
    }

    @Test
    void test_response_without_schema_type_is_avro()
    {
        RegisteredSchema schema = SchemaRegistryClient.fromResponse(3, Map.of("schema", "{\"type\":\"string\"}"));
        assertEquals(new RegisteredSchema(3, "AVRO", "{\"type\":\"string\"}"), schema);
    }

    @Test
    void test_response_schema_types()
    {
        assertEquals("JSON", SchemaRegistryClient.fromResponse(1, Map.of("schema", "{}", "schemaType", "JSON"))
                .schemaType());
        assertEquals("PROTOBUF", SchemaRegistryClient.fromResponse(1, Map.of("schema", "syntax = \"proto3\";", "schemaType", "PROTOBUF"))
                .schemaType());
        assertEquals("AVRO", SchemaRegistryClient.fromResponse(1, Map.of("schema", "{}", "schemaType", "AVRO"))
                .schemaType());
        assertEquals("JSON", SchemaRegistryClient.fromResponse(1, Map.of("schema", "{}", "schemaType", "json"))
                .schemaType());
    }

    @Test
    void test_response_null_fields()
    {
        Map<String, Object> response = new HashMap<>();
        response.put("schemaType", null);
        RegisteredSchema schema = SchemaRegistryClient.fromResponse(1, response);
        assertEquals("AVRO", schema.schemaType());
        assertNull(schema.schema());
    }

    @Test
    void test_failed_lookup_is_remembered()
    {
        // Port 9 (discard) on localhost: connection refused, no network needed
        SchemaRegistryClient client = new SchemaRegistryClient("http://127.0.0.1:9");
        RuntimeException first = assertThrows(RuntimeException.class, () -> client.get(5));
        assertEquals("Failed to fetch schema id 5 from all Schema Registry URLs: [http://127.0.0.1:9]", first.getMessage());

        // Same failure without a new HTTP call
        assertSame(first, assertThrows(RuntimeException.class, () -> client.get(5)));

        // Other ids are looked up on their own
        RuntimeException other = assertThrows(RuntimeException.class, () -> client.get(6));
        assertNotSame(first, other);
    }
}
