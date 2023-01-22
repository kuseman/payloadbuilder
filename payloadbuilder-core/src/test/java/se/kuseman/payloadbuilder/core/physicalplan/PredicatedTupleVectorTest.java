package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;

/** Test of {@link PredicatedTupleVector} */
public class PredicatedTupleVectorTest extends APhysicalPlanTest
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_error_non_boolean_filter()
    {
        TupleVector target = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Int), 0)));
        ValueVector filter = vv(ResolvedType.of(Type.Int), 10);
        new PredicatedTupleVector(target, filter);
    }

    @Test
    public void test_no_filtered_rows()
    {
        TupleVector target = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 0)));
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), false);
        assertEquals(0, new PredicatedTupleVector(target, filter).getRowCount());
    }

    @Test
    public void test_target_schema()
    {
        TupleVector target = TupleVector.of(schema("col1", "col2", "col3"), asList(vv(ResolvedType.of(Type.Any), 0, 1, 2, 3, 4), vv(ResolvedType.of(Type.Any), true, false, true, false, null),
                vv(ResolvedType.of(Type.Any), "string0", "string1", "string2", "string3", "string4")));
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true, false, true, null, true);
        PredicatedTupleVector actual = new PredicatedTupleVector(target, filter);

        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(3, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3"), actual.getSchema());
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 2, 4), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), true, true, null), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), "string0", "string2", "string4"), actual.getColumn(2));
    }

    @Test
    public void test_random_access()
    {
        TupleVector target = TupleVector.of(schema("col1", "col2", "col3"), asList(vv(ResolvedType.of(Type.Any), 0, 1, 2, 3, 4), vv(ResolvedType.of(Type.Any), true, false, true, false, null),
                vv(ResolvedType.of(Type.Any), "string0", "string1", "string2", "string3", "string4")));
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true, false, true, null, true);
        PredicatedTupleVector actual = new PredicatedTupleVector(target, filter);

        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(3, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3"), actual.getSchema());

        ValueVector vv = actual.getColumn(0);
        assertEquals(2, vv.getValue(1));
        assertEquals(4, vv.getValue(2));
        assertEquals(0, vv.getValue(0));

        vv = actual.getColumn(1);
        assertEquals(true, vv.getValue(1));
        assertEquals(null, vv.getValue(2));
        assertEquals(true, vv.getValue(0));

        vv = actual.getColumn(2);
        assertEquals("string2", vv.getValue(1));
        assertEquals("string4", vv.getValue(2));
        assertEquals("string0", vv.getValue(0));
    }

    @Test
    public void test_extended_schema()
    {
        Schema schema = schema(new Type[] { Type.Any, Type.Int, Type.Float }, "col1", "col2", "col3");

        TupleVector target = TupleVector.of(schema("col1"), asList(vv(ResolvedType.of(Type.Any), 0, 1, 2)));
        ValueVector filter = vv(ResolvedType.of(Type.Boolean), true, null, true);

        PredicatedTupleVector actual = new PredicatedTupleVector(target, schema, filter);

        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema(new Type[] { Type.Any, Type.Int, Type.Float }, "col1", "col2", "col3"), actual.getSchema());
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 2), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), null, null), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), null, null), actual.getColumn(2));
    }
}
