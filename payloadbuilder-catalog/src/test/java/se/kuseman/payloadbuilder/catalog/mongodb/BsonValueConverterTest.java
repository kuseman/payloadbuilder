package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Unit tests of {@link BsonValueConverter}. */
class BsonValueConverterTest
{
    // fromBson

    @Test
    void test_fromBson_null()
    {
        assertNull(BsonValueConverter.fromBson(null));
    }

    @Test
    void test_fromBson_objectId_to_hex_string()
    {
        ObjectId id = new ObjectId();
        assertEquals(id.toHexString(), BsonValueConverter.fromBson(id));
    }

    @Test
    void test_fromBson_decimal128_to_bigdecimal()
    {
        Decimal128 d = new Decimal128(new BigDecimal("12.3400"));
        assertEquals(new BigDecimal("12.3400"), BsonValueConverter.fromBson(d));
    }

    @Test
    void test_fromBson_date_to_epochDateTime()
    {
        Date date = new Date(1_700_000_000_000L);
        Object result = BsonValueConverter.fromBson(date);
        assertTrue(result instanceof EpochDateTime);
        assertEquals(1_700_000_000_000L, ((EpochDateTime) result).getEpoch());
    }

    @Test
    void test_fromBson_binary_to_byte_array()
    {
        byte[] bytes = { 1, 2, 3 };
        Binary binary = new Binary(bytes);
        assertArrayEquals(bytes, (byte[]) BsonValueConverter.fromBson(binary));
    }

    @Test
    void test_fromBson_passthrough_for_plain_types()
    {
        assertEquals("value", BsonValueConverter.fromBson("value"));
        assertEquals(123, BsonValueConverter.fromBson(123));
        assertEquals(123L, BsonValueConverter.fromBson(123L));
        assertEquals(true, BsonValueConverter.fromBson(true));
        assertEquals(1.5d, BsonValueConverter.fromBson(1.5d));
    }

    @Test
    void test_fromBson_nested_document_converts_recursively()
    {
        ObjectId nestedId = new ObjectId();
        Document doc = new Document("plain", "value").append("id", nestedId)
                .append("nested", new Document("key", 1));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) BsonValueConverter.fromBson(doc);

        assertEquals("value", result.get("plain"));
        assertEquals(nestedId.toHexString(), result.get("id"));
        assertEquals(Map.of("key", 1), result.get("nested"));
    }

    @Test
    void test_fromBson_list_converts_each_element_recursively()
    {
        ObjectId id = new ObjectId();
        List<Object> list = List.of(1, id, new Document("key", "value"));

        @SuppressWarnings("unchecked")
        List<Object> result = (List<Object>) BsonValueConverter.fromBson(list);

        assertEquals(1, result.get(0));
        assertEquals(id.toHexString(), result.get(1));
        assertEquals(Map.of("key", "value"), result.get(2));
    }

    @Test
    void test_fromBson_list_nested_inside_document_converts_recursively()
    {
        ObjectId id = new ObjectId();
        Document doc = new Document("items", List.of(id, "plain"));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) BsonValueConverter.fromBson(doc);
        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) result.get("items");

        assertEquals(id.toHexString(), items.get(0));
        assertEquals("plain", items.get(1));
    }

    // toBson

    @Test
    void test_toBson_null()
    {
        assertNull(BsonValueConverter.toBson(null, "field"));
    }

    @Test
    void test_toBson_utf8String_unwrapped_to_java_string()
    {
        assertEquals("hello", BsonValueConverter.toBson(UTF8String.from("hello"), "field"));
    }

    @Test
    void test_toBson_id_field_with_valid_hex_string_becomes_objectId()
    {
        String hex = "507f1f77bcf86cd799439011";
        assertEquals(new ObjectId(hex), BsonValueConverter.toBson(hex, "_id"));
    }

    @Test
    void test_toBson_id_field_with_non_hex_string_stays_a_string()
    {
        assertEquals("not-an-object-id", BsonValueConverter.toBson("not-an-object-id", "_id"));
    }

    @Test
    void test_toBson_hex_looking_string_on_non_id_field_stays_a_string()
    {
        String hex = "507f1f77bcf86cd799439011";
        assertEquals(hex, BsonValueConverter.toBson(hex, "otherField"));
    }

    @Test
    void test_toBson_epochDateTime_to_date()
    {
        EpochDateTime dt = EpochDateTime.from(1_700_000_000_000L);
        assertEquals(new Date(1_700_000_000_000L), BsonValueConverter.toBson(dt, "field"));
    }

    @Test
    void test_toBson_epochDateTimeOffset_to_date()
    {
        EpochDateTimeOffset dt = EpochDateTimeOffset.from(1_700_000_000_000L);
        assertEquals(new Date(1_700_000_000_000L), BsonValueConverter.toBson(dt, "field"));
    }

    @Test
    void test_toBson_decimal_to_decimal128()
    {
        Decimal decimal = Decimal.from(12.34);
        Object result = BsonValueConverter.toBson(decimal, "field");
        assertTrue(result instanceof Decimal128);
        assertEquals(decimal.asBigDecimal(), ((Decimal128) result).bigDecimalValue());
    }

    @Test
    void test_toBson_passthrough_for_plain_types()
    {
        assertEquals(123, BsonValueConverter.toBson(123, "field"));
        assertEquals(true, BsonValueConverter.toBson(true, "field"));
    }
}
