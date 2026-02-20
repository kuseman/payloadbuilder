package se.kuseman.payloadbuilder.bytes;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Tests for {@link SchemaReconciliationCache} and its wiring into {@link PayloadReader}/{@link VectorFactory}. */
class SchemaReconciliationCacheTest
{
    private static Column col(String name, Column.Type type, int id)
    {
        return new Column(name, ResolvedType.of(type), new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    @Test
    void test_cache_hit_reuses_reconciled_schema_and_reads_correct_values()
    {
        // @formatter:off
        Schema readerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2));

        Schema writerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("c", Type.Float, 3),     // inserted in the middle - forces reconciliation on every read
                col("b", Type.String, 2));
        // @formatter:on

        TupleVector written = TupleVector.of(writerSchema, asList(vv(Type.Int, 1, 2), vv(Type.Float, 1.1F, 2.2F), vv(Type.String, "one", "two")));
        byte[] bytesA = PayloadWriter.write(ValueVector.literalTable(written, 1));

        // A second, independent payload sharing the exact same writer schema shape (different row data)
        TupleVector written2 = TupleVector.of(writerSchema, asList(vv(Type.Int, 10, 20), vv(Type.Float, 9.9F, 8.8F), vv(Type.String, "ten", "twenty")));
        byte[] bytesB = PayloadWriter.write(ValueVector.literalTable(written2, 1));

        SchemaReconciliationCache cache = new SchemaReconciliationCache(16);

        TupleVector actualA = PayloadReader.readTupleVector(bytesA, readerSchema, false, cache);
        assertEquals(0, cache.hitCount());
        assertEquals(1, cache.missCount());
        assertEquals(1, cache.size());
        assertVectorsEquals(vv(Type.Int, 1, 2), actualA.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two"), actualA.getColumn(1));

        // Same schema shape again (different payload/data) -> cache hit, no new entry, still correct values
        TupleVector actualB = PayloadReader.readTupleVector(bytesB, readerSchema, false, cache);
        assertEquals(1, cache.hitCount());
        assertEquals(1, cache.missCount());
        assertEquals(1, cache.size());
        assertVectorsEquals(vv(Type.Int, 10, 20), actualB.getColumn(0));
        assertVectorsEquals(vv(Type.String, "ten", "twenty"), actualB.getColumn(1));
    }

    @Test
    void test_no_cache_behaves_exactly_like_the_three_arg_overload()
    {
        // @formatter:off
        Schema readerSchema = Schema.of(col("a", Type.Int, 1));
        Schema writerSchema = Schema.of(col("a", Type.Int, 1), col("b", Type.String, 2));
        // @formatter:on

        TupleVector written = TupleVector.of(writerSchema, asList(vv(Type.Int, 1, 2), vv(Type.String, "one", "two")));
        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector viaThreeArg = PayloadReader.readTupleVector(bytes, readerSchema, false);
        TupleVector viaNullCache = PayloadReader.readTupleVector(bytes, readerSchema, false, null);

        assertVectorsEquals(viaThreeArg.getColumn(0), viaNullCache.getColumn(0));
    }

    @Test
    void test_different_schema_instances_do_not_share_cache_entries_even_if_equal()
    {
        // Keying is on Schema *identity* on purpose (see class javadoc) - two structurally equal but distinct
        // Schema instances must each get their own cache entry, not share one.
        // @formatter:off
        Schema readerSchemaA = Schema.of(col("a", Type.Int, 1), col("b", Type.String, 2));
        Schema readerSchemaB = Schema.of(col("a", Type.Int, 1), col("b", Type.String, 2)); // equal content, different instance
        Schema writerSchema = Schema.of(col("a", Type.Int, 1), col("c", Type.Float, 3), col("b", Type.String, 2));
        // @formatter:on

        assertEquals(readerSchemaA, readerSchemaB);

        TupleVector written = TupleVector.of(writerSchema, asList(vv(Type.Int, 1), vv(Type.Float, 1.1F), vv(Type.String, "one")));
        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        SchemaReconciliationCache cache = new SchemaReconciliationCache(16);

        PayloadReader.readTupleVector(bytes, readerSchemaA, false, cache);
        assertEquals(0, cache.hitCount());
        assertEquals(1, cache.missCount());

        PayloadReader.readTupleVector(bytes, readerSchemaB, false, cache);
        assertEquals(0, cache.hitCount());
        assertEquals(2, cache.missCount());
        assertEquals(2, cache.size());
    }

    @Test
    void test_expand_flag_is_part_of_the_cache_key()
    {
        // @formatter:off
        Schema readerSchema = Schema.of(col("a", Type.Int, 1), col("b", Type.String, 2));
        Schema writerSchema = Schema.of(col("a", Type.Int, 1), col("c", Type.Float, 3), col("b", Type.String, 2));
        // @formatter:on

        TupleVector written = TupleVector.of(writerSchema, asList(vv(Type.Int, 1), vv(Type.Float, 1.1F), vv(Type.String, "one")));
        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        SchemaReconciliationCache cache = new SchemaReconciliationCache(16);

        TupleVector noExpand = PayloadReader.readTupleVector(bytes, readerSchema, false, cache);
        assertEquals(2, noExpand.getSchema()
                .getSize());
        assertEquals(1, cache.missCount());

        TupleVector expand = PayloadReader.readTupleVector(bytes, readerSchema, true, cache);
        assertEquals(3, expand.getSchema()
                .getSize());
        // Different expand flag -> different cache entry, not a hit against the non-expand result
        assertEquals(2, cache.missCount());
        assertEquals(2, cache.size());
    }

    @Test
    void test_maxSize_stops_caching_new_entries_without_evicting_existing_ones()
    {
        SchemaReconciliationCache cache = new SchemaReconciliationCache(1);

        // @formatter:off
        Schema readerSchema1 = Schema.of(col("a", Type.Int, 1), col("b", Type.String, 2));
        Schema writerSchema1 = Schema.of(col("a", Type.Int, 1), col("c", Type.Float, 3), col("b", Type.String, 2));
        Schema readerSchema2 = Schema.of(col("x", Type.Int, 10), col("y", Type.String, 20));
        Schema writerSchema2 = Schema.of(col("x", Type.Int, 10), col("z", Type.Float, 30), col("y", Type.String, 20));
        // @formatter:on

        byte[] bytes1 = PayloadWriter.write(ValueVector.literalTable(TupleVector.of(writerSchema1, asList(vv(Type.Int, 1), vv(Type.Float, 1.1F), vv(Type.String, "one"))), 1));
        byte[] bytes2 = PayloadWriter.write(ValueVector.literalTable(TupleVector.of(writerSchema2, asList(vv(Type.Int, 2), vv(Type.Float, 2.2F), vv(Type.String, "two"))), 1));

        PayloadReader.readTupleVector(bytes1, readerSchema1, false, cache);
        assertEquals(1, cache.size());

        // Second, distinct schema shape - cache is already at capacity, so this miss doesn't get cached
        PayloadReader.readTupleVector(bytes2, readerSchema2, false, cache);
        assertEquals(1, cache.size());
        assertEquals(0, cache.hitCount());
        assertEquals(2, cache.missCount());

        // The original entry is still there and still serves hits
        PayloadReader.readTupleVector(bytes1, readerSchema1, false, cache);
        assertEquals(1, cache.hitCount());
    }

    @Test
    void test_negative_maxSize_throws()
    {
        assertThrows(IllegalArgumentException.class, () -> new SchemaReconciliationCache(0));
        assertThrows(IllegalArgumentException.class, () -> new SchemaReconciliationCache(-1));
    }
}
