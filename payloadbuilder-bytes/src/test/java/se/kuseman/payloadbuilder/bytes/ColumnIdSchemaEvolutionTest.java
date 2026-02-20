package se.kuseman.payloadbuilder.bytes;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertTupleVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Tests for column-id based schema reconciliation. Verifies that inserting/removing/reordering columns in the writer's schema doesn't corrupt what an older/newer reader schema resolves, as long as
 * every column involved carries a stable {@link MetaData#COLUMN_ID}.
 */
class ColumnIdSchemaEvolutionTest
{
    private static Column col(String name, Column.Type type, int id)
    {
        return new Column(name, ResolvedType.of(type), new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    private static Column col(String name, ResolvedType type, int id)
    {
        return new Column(name, type, new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    @Test
    void test_no_drift_round_trip()
    {
        // @formatter:off
        Schema schema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2),
                col("c", Type.Float, 3));

        TupleVector vector = TupleVector.of(schema, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.String, "one", "two", "three"),
                vv(Type.Float, 1.1F, 2.2F, 3.3F)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vector, 1));

        assertTupleVectorsEquals(vector, PayloadReader.readTupleVector(bytes, schema, false));
        assertTupleVectorsEquals(vector, PayloadReader.readTupleVector(bytes, schema, true));
    }

    @Test
    void test_column_inserted_in_the_middle()
    {
        // @formatter:off
        Schema readerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2));

        Schema writerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("c", Type.Float, 3),     // <-- new column, inserted in the middle on the write side
                col("b", Type.String, 2));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.Float, 1.1F, 2.2F, 3.3F),
                vv(Type.String, "one", "two", "three")));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        // Old reader, unaware of "c" -> must still get correct "a"/"b" values, ignoring "c" entirely
        TupleVector oldReaderView = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertEquals(readerSchema, oldReaderView.getSchema());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), oldReaderView.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), oldReaderView.getColumn(1));

        // New reader, in the writer's exact shape -> full round trip
        assertTupleVectorsEquals(written, PayloadReader.readTupleVector(bytes, writerSchema, false));

        // Old reader with expand=true -> "c" appears too, name synthesized from its physical slot
        TupleVector expanded = PayloadReader.readTupleVector(bytes, readerSchema, true);
        assertEquals(3, expanded.getSchema()
                .getSize());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), expanded.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), expanded.getColumn(1));
        assertVectorsEquals(vv(Type.Float, 1.1F, 2.2F, 3.3F), expanded.getColumn(2));
    }

    @Test
    void test_column_removed_from_the_middle()
    {
        // @formatter:off
        // Old reader still expects "b", which the writer stopped producing
        Schema readerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.Boolean, 2),
                col("c", Type.String, 3));

        Schema writerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("c", Type.String, 3));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.String, "one", "two", "three")));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertEquals(readerSchema, actual.getSchema());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Boolean, null, null, null), actual.getColumn(1));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), actual.getColumn(2));
    }

    @Test
    void test_columns_reordered()
    {
        // @formatter:off
        Schema writerSchema = Schema.of(
                col("x", Type.Int, 1),
                col("y", Type.String, 2),
                col("z", Type.Float, 3));

        // Same ids, different logical order
        Schema readerSchema = Schema.of(
                col("z", Type.Float, 3),
                col("x", Type.Int, 1),
                col("y", Type.String, 2));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 10, 20, 30),
                vv(Type.String, "a", "b", "c"),
                vv(Type.Float, 1.5F, 2.5F, 3.5F)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertVectorsEquals(vv(Type.Float, 1.5F, 2.5F, 3.5F), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 10, 20, 30), actual.getColumn(1));
        assertVectorsEquals(vv(Type.String, "a", "b", "c"), actual.getColumn(2));
    }

    @Test
    void test_columns_reordered_with_identical_types()
    {
        // Deliberately same type on every column - test_columns_reordered's positions also differ in type,
        // meaning the pre-existing (pre-column-id) type-mismatch check alone would already have forced the
        // reconciliation path regardless of whether id-based position drift detection works at all. This test
        // isolates that: with every column typed Int, only the id-vs-position comparison in
        // Utils#validateResolvedType/expandType can detect the reorder - a type-only check would see three
        // matching Int columns and never flag a mismatch, silently reading position-swapped data.
        // @formatter:off
        Schema writerSchema = Schema.of(
                col("x", Type.Int, 1),
                col("y", Type.Int, 2),
                col("z", Type.Int, 3));

        Schema readerSchema = Schema.of(
                col("z", Type.Int, 3),
                col("x", Type.Int, 1),
                col("y", Type.Int, 2));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 10, 20, 30),
                vv(Type.Int, 100, 200, 300),
                vv(Type.Int, 1000, 2000, 3000)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertVectorsEquals(vv(Type.Int, 1000, 2000, 3000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 10, 20, 30), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Int, 100, 200, 300), actual.getColumn(2));
    }

    @Test
    void test_mixed_column_ids_throws()
    {
        // @formatter:off
        Schema schema = Schema.of(
                col("a", Type.Int, 1),
                Column.of("b", Type.String));
        // @formatter:on

        TupleVector vector = TupleVector.of(schema, asList(vv(Type.Int, 1), vv(Type.String, "one")));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> PayloadWriter.write(ValueVector.literalTable(vector, 1)));
        assertTrue(ex.getMessage()
                .contains("mixes columns"), ex.getMessage());
    }

    @Test
    void test_duplicate_column_ids_throws()
    {
        // @formatter:off
        Schema schema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 1));
        // @formatter:on

        TupleVector vector = TupleVector.of(schema, asList(vv(Type.Int, 1), vv(Type.String, "one")));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> PayloadWriter.write(ValueVector.literalTable(vector, 1)));
        assertTrue(ex.getMessage()
                .contains("Duplicate column id"), ex.getMessage());
    }

    @Test
    void test_nested_table_column_inserted_in_the_middle()
    {
        // @formatter:off
        Schema innerReaderSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sb", Type.String, 11));

        Schema innerWriterSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sc", Type.Float, 12),   // <-- inserted in the middle, only at the nested level
                col("sb", Type.String, 11));

        Schema outerSchema = Schema.of(
                col("id", Type.Int, 1),
                col("nested", ResolvedType.table(innerWriterSchema), 2));

        TupleVector innerVector = TupleVector.of(innerWriterSchema, asList(
                vv(Type.Int, 6, 7),
                vv(Type.Float, 1.1F, 2.2F),
                vv(Type.String, "x", "y")));

        TupleVector outerVector = TupleVector.of(outerSchema, asList(
                vv(Type.Int, 100),
                vv(ResolvedType.table(innerWriterSchema), innerVector)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(outerVector, 1));

        // Reader whose nested schema predates "sc"
        // @formatter:off
        Schema readerOuterSchema = Schema.of(
                col("id", Type.Int, 1),
                col("nested", ResolvedType.table(innerReaderSchema), 2));
        // @formatter:on

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerOuterSchema, false);
        assertVectorsEquals(vv(Type.Int, 100), actual.getColumn(0));

        TupleVector actualNested = actual.getColumn(1)
                .getTable(0);
        assertVectorsEquals(vv(Type.Int, 6, 7), actualNested.getColumn(0));
        assertVectorsEquals(vv(Type.String, "x", "y"), actualNested.getColumn(1));
    }

    @Test
    void test_array_of_table_with_column_inserted_in_the_middle()
    {
        // @formatter:off
        Schema innerWriterSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sc", Type.Float, 12),
                col("sb", Type.String, 11));

        TupleVector row0 = TupleVector.of(innerWriterSchema, asList(
                vv(Type.Int, 1),
                vv(Type.Float, 9.9F),
                vv(Type.String, "row0")));

        TupleVector row1 = TupleVector.of(innerWriterSchema, asList(
                vv(Type.Int, 2, 3),
                vv(Type.Float, 8.8F, 7.7F),
                vv(Type.String, "row1a", "row1b")));

        // Array<Table> column with two rows, each row's array holding a single table element
        ValueVector arrayColumn = vv(ResolvedType.array(ResolvedType.table(innerWriterSchema)), vv(ResolvedType.table(innerWriterSchema), row0),
                vv(ResolvedType.table(innerWriterSchema), row1));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(arrayColumn);

        ValueVector actual = PayloadReader.read(bytes);
        TupleVector actualRow0 = actual.getArray(0)
                .getTable(0);
        assertVectorsEquals(vv(Type.Int, 1), actualRow0.getColumn(0));
        assertVectorsEquals(vv(Type.String, "row0"), actualRow0.getColumn(2));

        TupleVector actualRow1 = actual.getArray(1)
                .getTable(0);
        assertVectorsEquals(vv(Type.Int, 2, 3), actualRow1.getColumn(0));
        assertVectorsEquals(vv(Type.String, "row1a", "row1b"), actualRow1.getColumn(2));
    }

    @Test
    void test_reader_schema_without_ids_falls_back_to_positional()
    {
        // Writer schema is id-tagged, reader schema (built independently, no ids) isn't. Id mode requires both
        // sides, so this must gracefully fall back to legacy positional matching rather than throw.
        // @formatter:off
        Schema writerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2));

        Schema readerSchema = Schema.of(
                Column.of("a", Type.Int),
                Column.of("b", Type.String));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.String, "one", "two", "three")));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), actual.getColumn(1));
    }

    @Test
    void test_writer_schema_without_ids_falls_back_to_positional()
    {
        // Payload written before ids were adopted, reader schema now has ids. Must still fall back gracefully.
        // @formatter:off
        Schema writerSchema = Schema.of(
                Column.of("a", Type.Int),
                Column.of("b", Type.String));

        Schema readerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.String, "one", "two", "three")));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), actual.getColumn(1));
    }

    @Test
    void test_expand_adds_unknown_payload_columns_by_id()
    {
        // @formatter:off
        Schema readerSchema = Schema.of(
                col("a", Type.Int, 1));

        Schema writerSchema = Schema.of(
                col("a", Type.Int, 1),
                col("b", Type.String, 2),
                col("c", Type.Float, 3));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(Type.Int, 1, 2),
                vv(Type.String, "one", "two"),
                vv(Type.Float, 1.1F, 2.2F)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, true);
        assertEquals(3, actual.getSchema()
                .getSize());
        assertVectorsEquals(vv(Type.Int, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.String, "one", "two"), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Float, 1.1F, 2.2F), actual.getColumn(2));
    }

    @Test
    void test_nested_object_column_inserted_in_the_middle()
    {
        // Same as test_nested_table_column_inserted_in_the_middle but for Type.Object, which goes through a
        // different writer/reader class pair (ObjectVectorWriter/ObjectVector) sharing the same underlying
        // writeTupleVector/BytesTupleVector machinery - verified independently since nothing guarantees the two
        // stay in sync just because Table was tested.
        // @formatter:off
        Schema innerReaderSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sb", Type.String, 11));

        Schema innerWriterSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sc", Type.Float, 12),   // <-- inserted in the middle, only at the nested level
                col("sb", Type.String, 11));

        Schema outerSchema = Schema.of(
                col("id", Type.Int, 1),
                col("nested", ResolvedType.object(innerWriterSchema), 2));

        ObjectVector nestedObject = ObjectVector.wrap(TupleVector.of(innerWriterSchema, asList(
                vv(Type.Int, 6),
                vv(Type.Float, 1.1F),
                vv(Type.String, "x"))));

        TupleVector outerVector = TupleVector.of(outerSchema, asList(
                vv(Type.Int, 100),
                vv(ResolvedType.object(innerWriterSchema), nestedObject)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(outerVector, 1));

        // Reader whose nested schema predates "sc"
        // @formatter:off
        Schema readerOuterSchema = Schema.of(
                col("id", Type.Int, 1),
                col("nested", ResolvedType.object(innerReaderSchema), 2));
        // @formatter:on

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerOuterSchema, false);
        assertVectorsEquals(vv(Type.Int, 100), actual.getColumn(0));

        ObjectVector actualObject = actual.getColumn(1)
                .getObject(0);
        assertEquals(6, actualObject.getValue(0)
                .getInt(actualObject.getRow()));
        assertEquals("x", actualObject.getValue(1)
                .getString(actualObject.getRow())
                .toString());
    }

    @Test
    void test_id_matched_column_with_type_drift_and_reorder()
    {
        // Mirrors the classic "obsolete column repurposed with a new type" migration flow from
        // PayloadReader#readTupleVector's doc comment, but driven by column id instead of position - the same
        // id can be reordered in the writer's schema over time AND change type, and an old reader must still
        // get correct values for its unaffected columns plus a safe, castable view of the repurposed one.
        // @formatter:off
        Schema readerSchemaV1 = Schema.of(
                col("int", Type.Int, 1),
                col("arr", ResolvedType.array(Column.Type.Float), 2),
                col("str1", Type.String, 3));

        // Writer has since: reordered "str1" before the repurposed column, changed id=2's type from
        // Array<Float> to String (keeping its id, simulating a corrected column), and appended a new column
        Schema writerSchemaV2 = Schema.of(
                col("int", Type.Int, 1),
                col("str1", Type.String, 3),
                col("str", Type.String, 2),
                col("arrInt", ResolvedType.array(Column.Type.Int), 4));

        TupleVector written = TupleVector.of(writerSchemaV2, asList(
                vv(Type.Int, 1, 2, 3),
                vv(Type.String, "one", "two", "three"),
                vv(Type.String, "hello", "world", null),
                vv(ResolvedType.array(Column.Type.Int),
                        vv(Type.Int, 1, 2),
                        vv(Type.Int, 3),
                        vv(Type.Int, 4, 5))));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchemaV1, false);

        // Schema handed back to the caller is still the reader's own (names/order unchanged)
        assertEquals(readerSchemaV1, actual.getSchema());

        assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        // Column "arr" is, per the payload, actually a String now - readable as such via implicit resolution
        assertVectorsEquals(vv(Type.String, "hello", "world", null), actual.getColumn(1));
        assertVectorsEquals(vv(Type.String, "one", "two", "three"), actual.getColumn(2));

        // Accessing it as the old, no-longer-true type must fail loudly rather than silently corrupt
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> actual.getColumn(1)
                .getArray(0));
        assertTrue(ex.getMessage()
                .contains("Cannot cast String to Array"), ex.getMessage());
    }

    @Test
    void test_array_nesting_depth_mismatch_payload_shallower_than_expected()
    {
        // Reader expects Array<Array<Int>> (nested twice), payload actually only wrote Array<Int> (nested once) -
        // reconcileColumn's array-digging loop must detect the depth divergence and fall back to the payload's
        // actual (shallower) type rather than mis-nest or crash.
        // @formatter:off
        Schema readerSchema = Schema.of(
                col("arr", ResolvedType.array(ResolvedType.array(Column.Type.Int)), 1));

        Schema writerSchema = Schema.of(
                col("arr", ResolvedType.array(Column.Type.Int), 1));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(ResolvedType.array(Column.Type.Int), vv(Type.Int, 1, 2, 3))));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        ValueVector column = actual.getColumn(0);
        assertEquals(ResolvedType.array(Column.Type.Int), column.type());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3), column.getArray(0));
    }

    @Test
    void test_array_nesting_depth_mismatch_payload_deeper_than_expected()
    {
        // Mirror of the above - reader expects Array<Int>, payload actually wrote Array<Array<Int>>.
        // @formatter:off
        Schema readerSchema = Schema.of(
                col("arr", ResolvedType.array(Column.Type.Int), 1));

        Schema writerSchema = Schema.of(
                col("arr", ResolvedType.array(ResolvedType.array(Column.Type.Int)), 1));

        TupleVector written = TupleVector.of(writerSchema, asList(
                vv(ResolvedType.array(ResolvedType.array(Column.Type.Int)), vv(ResolvedType.array(Column.Type.Int), vv(Type.Int, 4, 5)))));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        ValueVector column = actual.getColumn(0);
        assertEquals(ResolvedType.array(ResolvedType.array(Column.Type.Int)), column.type());
        assertVectorsEquals(vv(Type.Int, 4, 5), column.getArray(0)
                .getArray(0));
    }

    @Test
    void test_triple_nested_array_of_table_with_column_inserted_in_the_middle()
    {
        // Array<Array<Array<Table>>>> - three levels of array nesting on top of a table whose own schema has
        // drifted (column inserted in the middle), combined with id-based reconciliation throughout. This is
        // the deepest combination this library supports and the one most likely to expose an off-by-one in the
        // nestCount unwrap/rewrap bookkeeping.
        // @formatter:off
        Schema innerReaderSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sb", Type.String, 11));

        Schema innerWriterSchema = Schema.of(
                col("sa", Type.Int, 10),
                col("sc", Type.Float, 12),
                col("sb", Type.String, 11));

        ResolvedType readerArrayType = ResolvedType.array(ResolvedType.array(ResolvedType.array(ResolvedType.table(innerReaderSchema))));
        ResolvedType writerArrayType = ResolvedType.array(ResolvedType.array(ResolvedType.array(ResolvedType.table(innerWriterSchema))));

        Schema readerSchema = Schema.of(col("arr", readerArrayType, 1));
        Schema writerSchema = Schema.of(col("arr", writerArrayType, 1));

        TupleVector innerRow = TupleVector.of(innerWriterSchema, asList(
                vv(Type.Int, 6),
                vv(Type.Float, 1.1F),
                vv(Type.String, "x")));

        // Array<Array<Array<Table>>>> with a single element at every level - one vv() wrap per Array(, plus one
        // for the Table itself, matching writerArrayType's 3 levels of array nesting exactly
        ValueVector tableVec = vv(ResolvedType.table(innerWriterSchema), innerRow);
        ValueVector arrayLevel3 = vv(ResolvedType.array(ResolvedType.table(innerWriterSchema)), tableVec);
        ValueVector arrayLevel2 = vv(ResolvedType.array(ResolvedType.array(ResolvedType.table(innerWriterSchema))), arrayLevel3);
        ValueVector arrayLevel1 = vv(writerArrayType, arrayLevel2);

        TupleVector written = TupleVector.of(writerSchema, asList(arrayLevel1));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(written, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, readerSchema, false);
        // Structural check only - the reconciled inner schema's columns carry internal physical-slot bookkeeping
        // metadata rather than the original column ids, so comparing the full ResolvedType (including metadata)
        // against readerArrayType isn't the right check here; what matters is the shape and the actual data below
        assertEquals(Column.Type.Array, actual.getColumn(0)
                .type()
                .getType());

        TupleVector actualInner = actual.getColumn(0)
                .getArray(0)
                .getArray(0)
                .getArray(0)
                .getTable(0);
        assertVectorsEquals(vv(Type.Int, 6), actualInner.getColumn(0));
        assertVectorsEquals(vv(Type.String, "x"), actualInner.getColumn(1));
    }
}
