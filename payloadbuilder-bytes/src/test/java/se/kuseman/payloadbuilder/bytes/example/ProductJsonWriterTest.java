package se.kuseman.payloadbuilder.bytes.example;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter;

/**
 * Verifies {@link ProductJsonWriter} produces correct JSON entirely through the raw-UTF8-bytes path - including a name with multi-byte characters, to prove the {@code arraycopy} out of the payload's
 * backing array (no String/char[] materialization) round-trips correctly, not just ASCII.
 */
class ProductJsonWriterTest
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Test
    void test_write_product_as_json_via_output_stream() throws IOException
    {
        // @formatter:off
        TupleVector skus = TupleVector.of(SkuSchema.SCHEMA, asList(
                vv(Type.Int, 101, 102),
                vv(Type.String, "Small", "Blå/Röd"),   // multi-byte utf8 characters on purpose
                vv(Type.Decimal, new BigDecimal("19.99"), new BigDecimal("21.99"))));

        TupleVector product = TupleVector.of(ProductSchema.SCHEMA, asList(
                vv(Type.Int, 1),
                vv(Type.String, "Tröja"),               // multi-byte utf8 characters on purpose
                vv(Type.Decimal, new BigDecimal("19.99")),
                vv(ResolvedType.table(SkuSchema.SCHEMA), skus)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(product, 1));
        IProduct actual = Products.fromBytes(bytes);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Must be an OutputStream-backed generator (UTF8JsonGenerator) to actually get the raw byte fast path -
        // a Writer-backed generator would just decode internally, losing the point of this whole exercise
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8))
        {
            ProductJsonWriter.write(generator, actual);
        }

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.contains("\"Tröja\""), json);
        assertTrue(json.contains("\"Blå/Röd\""), json);
        assertTrue(json.startsWith("{\"id\":1,\"name\":\"Tröja\""), json);
        assertTrue(json.contains("\"skus\":[{\"id\":101,\"name\":\"Small\",\"price\":19.99},{\"id\":102,\"name\":\"Blå/Röd\",\"price\":21.99}]"), json);

        // Parse it back via jackson-core's streaming API (no databind dependency in this module) to verify
        // structural + value correctness, not just "contains the right substrings"
        try (JsonParser parser = JSON_FACTORY.createParser(out.toByteArray()))
        {
            parser.nextToken(); // START_OBJECT
            parser.nextValue(); // id
            assertEquals(1, parser.getIntValue());
            parser.nextValue(); // name
            assertEquals("Tröja", parser.getText());
            parser.nextValue(); // price
            assertEquals(new BigDecimal("19.99"), parser.getDecimalValue());
            parser.nextToken(); // FIELD_NAME "skus"
            parser.nextToken(); // START_ARRAY
            parser.nextToken(); // START_OBJECT (first sku)
            parser.nextValue(); // id
            assertEquals(101, parser.getIntValue());
            parser.nextValue(); // name
            assertEquals("Small", parser.getText());
        }
    }

    @Test
    void test_thread_local_buffer_is_reused_and_grows() throws IOException
    {
        // Writing a short name after a long one must not leave stale bytes from the longer name behind - the
        // writer must always pass the actual length, not the buffer's (possibly larger, reused) length
        // @formatter:off
        TupleVector skusLong = TupleVector.of(SkuSchema.SCHEMA, asList(
                vv(Type.Int, 1),
                vv(Type.String, "A very very very long sku name indeed"),
                vv(Type.Decimal, new BigDecimal("1.00"))));
        TupleVector productLong = TupleVector.of(ProductSchema.SCHEMA, asList(
                vv(Type.Int, 1),
                vv(Type.String, "A very very very long product name indeed"),
                vv(Type.Decimal, new BigDecimal("1.00")),
                vv(ResolvedType.table(SkuSchema.SCHEMA), skusLong)));

        TupleVector skusShort = TupleVector.of(SkuSchema.SCHEMA, asList(
                vv(Type.Int, 2),
                vv(Type.String, "S"),
                vv(Type.Decimal, new BigDecimal("2.00"))));
        TupleVector productShort = TupleVector.of(ProductSchema.SCHEMA, asList(
                vv(Type.Int, 2),
                vv(Type.String, "P"),
                vv(Type.Decimal, new BigDecimal("2.00")),
                vv(ResolvedType.table(SkuSchema.SCHEMA), skusShort)));
        // @formatter:on

        IProduct longProduct = Products.fromBytes(PayloadWriter.write(ValueVector.literalTable(productLong, 1)));
        IProduct shortProduct = Products.fromBytes(PayloadWriter.write(ValueVector.literalTable(productShort, 1)));

        // Same thread -> same static ThreadLocal buffer across both writes. The long name grows it; the short
        // name afterwards must only ever see its own bytes, not stale leftovers from the longer name it reused
        // the (larger) buffer from.
        ByteArrayOutputStream longOut = new ByteArrayOutputStream();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(longOut, JsonEncoding.UTF8))
        {
            ProductJsonWriter.write(generator, longProduct);
        }

        ByteArrayOutputStream shortOut = new ByteArrayOutputStream();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(shortOut, JsonEncoding.UTF8))
        {
            ProductJsonWriter.write(generator, shortProduct);
        }

        assertTrue(longOut.toString(StandardCharsets.UTF_8)
                .contains("\"name\":\"A very very very long product name indeed\""));
        assertEquals("{\"id\":2,\"name\":\"P\",\"price\":2.00,\"skus\":[{\"id\":2,\"name\":\"S\",\"price\":2.00}]}", shortOut.toString(StandardCharsets.UTF_8));
    }
}
