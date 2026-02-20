package se.kuseman.payloadbuilder.bytes.example;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter;

/**
 * Demonstrates building a typed, ergonomic accessor layer ({@link IProduct}/{@link ISku}) on top of the low level {@link ValueVector}/{@link TupleVector} API, and that it stays correct across schema
 * evolution as long as columns carry a stable {@link MetaData#COLUMN_ID}.
 */
class ProductsExampleTest
{
    @Test
    void test_typed_accessors_including_indexed_nested_sku_access()
    {
        // @formatter:off
        TupleVector skus = TupleVector.of(SkuSchema.SCHEMA, asList(
                vv(Type.Int, 101, 102, 103),
                vv(Type.String, "Small", "Medium", "Large"),
                vv(Type.Decimal, new BigDecimal("19.99"), new BigDecimal("21.99"), new BigDecimal("23.99"))));

        TupleVector product = TupleVector.of(ProductSchema.SCHEMA, asList(
                vv(Type.Int, 1),
                vv(Type.String, "T-Shirt"),
                vv(Type.Decimal, new BigDecimal("19.99")),
                vv(ResolvedType.table(SkuSchema.SCHEMA), skus)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(product, 1));

        // This is the whole point: a plain, typed, list-like API over what's really columnar binary data
        IProduct actual = Products.fromBytes(bytes);

        assertEquals(1, actual.getId());
        assertEquals("T-Shirt", actual.getName()
                .toString());
        assertEquals(new BigDecimal("19.99"), actual.getPrice()
                .asBigDecimal());

        List<ISku> actualSkus = actual.getSkus();
        assertEquals(3, actualSkus.size());

        // product.getSkus().get(2).getPrice() - random access into a specific sku, no other row or column touched
        assertEquals("Large", actualSkus.get(2)
                .getName()
                .toString());
        assertEquals(new BigDecimal("23.99"), actualSkus.get(2)
                .getPrice()
                .asBigDecimal());
    }

    @Test
    void test_reading_a_payload_written_before_a_sku_column_existed()
    {
        // The schema as it looked before "sku.name" existed - same ids as SkuSchema for the columns that survived.
        // A real "old writer" would have its own such schema constant from that point in time; this test just
        // inlines it to simulate that without needing a second source set.
        Schema oldSkuSchema = Schema.of(col("id", Type.Int, 1), col("price", Type.Decimal, 3));
        Schema oldProductSchema = Schema.of(col("id", Type.Int, 1), col("name", Type.String, 2), col("price", Type.Decimal, 3), col("skus", ResolvedType.table(oldSkuSchema), 4));

        // @formatter:off
        TupleVector oldSkus = TupleVector.of(oldSkuSchema, asList(
                vv(Type.Int, 201, 202),
                vv(Type.Decimal, new BigDecimal("9.99"), new BigDecimal("11.99"))));

        TupleVector oldProduct = TupleVector.of(oldProductSchema, asList(
                vv(Type.Int, 7),
                vv(Type.String, "Old Mug"),
                vv(Type.Decimal, new BigDecimal("9.99")),
                vv(ResolvedType.table(oldSkuSchema), oldSkus)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(oldProduct, 1));

        // Products.fromBytes always reads with today's SkuSchema, which does expect "name" - the typed layer
        // doesn't need to know or care that this particular payload predates it
        IProduct actual = Products.fromBytes(bytes);

        assertEquals(7, actual.getId());
        List<ISku> actualSkus = actual.getSkus();
        assertEquals(2, actualSkus.size());
        assertEquals(202, actualSkus.get(1)
                .getId());
        assertEquals(new BigDecimal("11.99"), actualSkus.get(1)
                .getPrice()
                .asBigDecimal());
        // Column didn't exist in this payload at all -> null, not an exception
        assertEquals(null, actualSkus.get(1)
                .getName());
    }

    private static Column col(String name, Type type, int id)
    {
        return new Column(name, ResolvedType.of(type), new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    private static Column col(String name, ResolvedType type, int id)
    {
        return new Column(name, type, new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }
}
