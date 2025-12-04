package se.kuseman.payloadbuilder.core.execution.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link ValueVector}. */
class TupleVectorTest
{
    @Test
    void test()
    {
        // Mismatch size schema and columns
        assertThrows(IllegalArgumentException.class, () -> TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(vv(Type.Int, 1), vv(Type.Float, 1))));

        // Columns un-equal sizes
        assertThrows(IllegalArgumentException.class, () -> TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float)), vv(Type.Int, 1, 2), vv(Type.Float, 1)));

        // Schema and columns type mismatch
        assertThrows(IllegalArgumentException.class, () -> TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Float)), vv(Type.Boolean, 1), vv(Type.Float, 1)));

        // No columns is ok since this will create an empty tuple vector with only a schema
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("col1", Type.Int))), TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of()));
    }

    @Test
    void test_nested_schema_with_any_and_typed_vectors()
    {
        //@formatter:off
        Schema schema = Schema.of(Column.of("obj", ResolvedType.object(Schema.of(
                Column.of("key1", Column.Type.Any),
                Column.of("key2", Column.Type.Any)
                ))));
        Schema vectorSchema = Schema.of(Column.of("obj", ResolvedType.object(Schema.of(
                Column.of("key1", Column.Type.Int),
                Column.of("key2", Column.Type.Float)
                ))));

        Schema vectorInnerSchema = vectorSchema.getColumns().get(0).getType().getSchema();

        ValueVector vector = vv(ResolvedType.object(vectorInnerSchema), ObjectVector.wrap(TupleVector.of(
                vectorInnerSchema,
                List.of(
                   ValueVector.literalInt(10, 1),
                   ValueVector.literalFloat(13.30F, 1)
                )
                )));
        //@formatter:on
        TupleVector actual = TupleVector.of(schema, vector);
        assertEquals("TupleVector", actual.toString());
        assertEquals(schema, actual.getSchema());
        assertEquals(1, actual.getRowCount());
        VectorTestUtils.assertVectorsEquals(vector, actual.getColumn(0));
    }

    @Test
    void test_nested_schema_mismatch_sizes()
    {
        //@formatter:off
        Schema schema = Schema.of(Column.of("obj", ResolvedType.object(Schema.of(
                Column.of("key1", Column.Type.Any),
                Column.of("key2", Column.Type.Any)
                ))));
        Schema vectorSchema = Schema.of(Column.of("obj", ResolvedType.object(Schema.of(
                Column.of("key1", Column.Type.Int),
                Column.of("key2", Column.Type.Float),
                Column.of("key3", Column.Type.Double)
                ))));

        Schema vectorInnerSchema = vectorSchema.getColumns().get(0).getType().getSchema();

        ValueVector vector = vv(ResolvedType.object(vectorInnerSchema), ObjectVector.wrap(TupleVector.of(
                vectorInnerSchema,
                List.of(
                   ValueVector.literalInt(10, 1),
                   ValueVector.literalFloat(13.30F, 1),
                   ValueVector.literalDouble(13.30D, 1)
                )
                )));
        //@formatter:on

        assertThrows(IllegalArgumentException.class, () -> TupleVector.of(schema, vector));
    }

    @Test
    void test_nested_schema_ok_with_empty_inner_schema()
    {
        //@formatter:off
        Schema schema = Schema.of(Column.of("obj", ResolvedType.object(Schema.of(
                Column.of("key1", Column.Type.Any),
                Column.of("key2", Column.Type.Any)
                ))));

        ValueVector vector = ValueVector.empty(ResolvedType.object(Schema.EMPTY));
        //@formatter:on
        TupleVector actual = TupleVector.of(schema, vector);
        assertEquals("TupleVector", actual.toString());
        assertEquals(schema, actual.getSchema());
        assertEquals(0, actual.getRowCount());
        VectorTestUtils.assertVectorsEquals(vector, actual.getColumn(0));
    }
}
