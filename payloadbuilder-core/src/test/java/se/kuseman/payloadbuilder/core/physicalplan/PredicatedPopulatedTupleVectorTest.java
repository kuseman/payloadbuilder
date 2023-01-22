package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;

/** Test of {@link PredicatedPopulatedTupleVector} */
public class PredicatedPopulatedTupleVectorTest extends APhysicalPlanTest
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_error_non_boolean_filter()
    {
        TupleVector outer = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0)));
        TupleVector inner = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0)));

        ValueVector filter = vv(ResolvedType.of(Type.Int), 10);
        new PredicatedPopulatedTupleVector(outer, inner, filter, "p");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_error_filter_has_wrong_size()
    {
        TupleVector outer = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0)));
        TupleVector inner = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0, 1)));

        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true);
        new PredicatedPopulatedTupleVector(outer, inner, filter, "p");
    }

    @Test
    public void test_no_filtered_rows()
    {
        TupleVector outer = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0)));
        TupleVector inner = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0, 1)));
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), false, null);
        assertEquals(0, new PredicatedPopulatedTupleVector(outer, inner, filter, "p").getRowCount());
    }

    @Test
    public void test_last_bit_set()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 2, 1), vv(ResolvedType.of(Type.Any), 1, 3, 2)));
        TupleVector inner = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));

        ValueVector filter = vv(ResolvedType.of(Type.Boolean), false, false, false, false, null, null, false, false, true);

        TupleVector actual = new PredicatedPopulatedTupleVector(outer, inner, filter, "p");

        assertEquals(1, actual.getRowCount());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 2), actual.getColumn(1));

        ValueVector pvv = actual.getColumn(2);
        assertEquals(1, pvv.size());
        assertEquals(ResolvedType.tupleVector(inner.getSchema()), pvv.type());

        actual = (TupleVector) pvv.getValue(0);
        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(1, actual.getRowCount());
        assertEquals(schema("col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 3), actual.getColumn(1));
    }

    @Test
    public void test()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 2, 1), vv(ResolvedType.of(Type.Any), 1, 3, 2)));
        TupleVector inner = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));
        /*
         * @formatter:off
         * 0 0 0 2 2 2 1 1 1 
         * 1 1 1 3 3 3 2 2 2
         * 
         * 0 0 1 0 0 1 0 0 1
         * 1 2 3 1 2 3 1 2 3
         * -----------------------------------
         * 1 1 0 0 0 0 1 0 1
         * 
         * @formatter:on
         */
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true, true, false, false, null, null, true, false, true);

        TupleVector actual = new PredicatedPopulatedTupleVector(outer, inner, filter, "a");

        Schema expected = new Schema(asList(col("col1", Type.Any), col("col2", Type.Any), new Column("a", ResolvedType.tupleVector(schema("col3", "col4")))));

        // Outer size plus one populated
        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(expected, actual.getSchema());

        // Two filtered outer rows
        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2), actual.getColumn(1));

        ValueVector pvv = actual.getColumn(2);
        assertEquals(2, pvv.size());
        assertEquals(ResolvedType.tupleVector(inner.getSchema()), pvv.type());

        // First populated vector
        actual = (TupleVector) pvv.getValue(0);
        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema("col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2), actual.getColumn(1));

        // Second populated vector
        actual = (TupleVector) pvv.getValue(1);
        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema("col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 3), actual.getColumn(1));
    }

    @Test
    public void test_random_access()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 2, 1), vv(ResolvedType.of(Type.Any), 1, 3, 2)));
        TupleVector inner = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));
        /*
         * 
         * 
         * @formatter:off
         * 0 0 0 2 2 2 1 1 1 
         * 1 1 1 3 3 3 2 2 2
         * 
         * 0 0 1 0 0 1 0 0 1
         * 1 2 3 1 2 3 1 2 3
         * -----------------------------------
         * 1 1 0 0 0 0 1 0 1
         * 
         * @formatter:on
         */
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true, true, false, false, null, null, true, false, true);

        TupleVector actual = new PredicatedPopulatedTupleVector(outer, inner, filter, "p");

        // Outer size plus one populated
        assertEquals(3, actual.getSchema()
                .getSize());

        // Two filtered outer rows
        assertEquals(2, actual.getRowCount());

        ValueVector vv = actual.getColumn(0);
        assertEquals(1, vv.getValue(1));
        assertEquals(0, vv.getValue(0));

        vv = actual.getColumn(1);
        assertEquals(2, vv.getValue(1));
        assertEquals(1, vv.getValue(0));

        ValueVector pvv = actual.getColumn(2);
        assertEquals(2, pvv.size());
        assertEquals(ResolvedType.tupleVector(inner.getSchema()), pvv.type());

        // First populated vector
        actual = (TupleVector) pvv.getValue(0);

        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema("col3", "col4"), actual.getSchema());

        vv = actual.getColumn(0);
        assertEquals(0, vv.getValue(1));
        assertEquals(0, vv.getValue(0));

        vv = actual.getColumn(1);
        assertEquals(2, vv.getValue(1));
        assertEquals(1, vv.getValue(0));

        // Second populated vector
        actual = (TupleVector) pvv.getValue(1);

        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema("col3", "col4"), actual.getSchema());

        vv = actual.getColumn(0);
        assertEquals(1, vv.getValue(1));
        assertEquals(0, vv.getValue(0));

        vv = actual.getColumn(1);
        assertEquals(3, vv.getValue(1));
        assertEquals(1, vv.getValue(0));
    }
}
