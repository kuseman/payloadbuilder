package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;

/** Test of {@link TupleVector} */
public class TupleVectorTest extends APhysicalPlanTest
{
    @Test
    public void test_write()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 1, 2), vv(ResolvedType.of(Type.Any), 3, 4)));
        TupleVector inner = TupleVector.of(schema(new Type[] { Type.Any, Type.Boolean }, "col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 5, 6), vv(ResolvedType.of(Type.Boolean), false, true)));
        TupleVector cartesian = VectorUtils.cartesian(outer, inner);

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        cartesian.write(w, context, false);
        w.close();

        // CSOFF
        assertEquals(
                "[{\"col1\":1,\"col2\":3,\"col3\":5,\"col4\":false},{\"col1\":1,\"col2\":3,\"col3\":6,\"col4\":true},{\"col1\":2,\"col2\":4,\"col3\":5,\"col4\":false},{\"col1\":2,\"col2\":4,\"col3\":6,\"col4\":true}]",
                writer.toString());
        // CSON
    }

    @Test
    public void test_write_1()
    {
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(
                Column.of("col3", ResolvedType.valueVector(ResolvedType.of(Type.Any))), 
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.Float)),
                Column.of("col7", ResolvedType.of(Type.Double)),
                Column.of("col8", ResolvedType.of(Type.String))),
                asList(
                        vv(ResolvedType.valueVector(ResolvedType.of(Type.Any)), vv(ResolvedType.of(Type.Any), 5, 6), vv(ResolvedType.of(Type.Any), 6, 7)),
                        vv(ResolvedType.of(Type.Boolean), false, true),
                        vv(ResolvedType.of(Type.Long), 1L, 2L),
                        vv(ResolvedType.of(Type.Float), 10.1F, 20.2F),
                        vv(ResolvedType.of(Type.Double), 100.10D, 200.20D),
                        vv(ResolvedType.of(Type.String), "one", "two")));
        //@formatter:on

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        vector.write(w, context, false);
        w.close();

        assertEquals(
                "[{\"col3\":[5,6],\"col4\":false,\"col5\":1,\"col6\":10.1,\"col7\":100.1,\"col8\":\"one\"},{\"col3\":[6,7],\"col4\":true,\"col5\":2,\"col6\":20.2,\"col7\":200.2,\"col8\":\"two\"}]",
                writer.toString());
    }

    @Test
    public void test_cartesian_same_count()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 1, 2), vv(ResolvedType.of(Type.Any), 3, 4)));
        TupleVector inner = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 5, 6), vv(ResolvedType.of(Type.Any), 7, 8)));
        TupleVector cartesian = VectorUtils.cartesian(outer, inner);

        assertEquals(4, cartesian.getSchema()
                .getSize());
        assertEquals(4, cartesian.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), cartesian.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 1, 2, 2), cartesian.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 3, 3, 4, 4), cartesian.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, 6, 5, 6), cartesian.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 7, 8, 7, 8), cartesian.getColumn(3));
    }

    @Test
    public void test_cartesian()
    {
        TupleVector outer = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector inner = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));

        TupleVector cartesian = VectorUtils.cartesian(outer, inner);

        assertEquals(4, cartesian.getSchema()
                .getSize());
        assertEquals(6, cartesian.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), cartesian.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 0, 1, 1, 1), cartesian.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 1, 1, 2, 2, 2), cartesian.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 1, 0, 0, 1), cartesian.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 3, 1, 2, 3), cartesian.getColumn(3));

        TupleVector inner2 = TupleVector.of(Schema.of(Column.of("col5", ResolvedType.of(Type.Any)), Column.of("col6", ResolvedType.of(Type.Any))),
                asList(vv(ResolvedType.of(Type.Any), true, false), vv(ResolvedType.of(Type.Any), "string1", "string2")));

        // Test a cartesian of a cartesian
        cartesian = VectorUtils.cartesian(cartesian, inner2);

        assertEquals(6, cartesian.getSchema()
                .getSize());
        assertEquals(12, cartesian.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4", "col5", "col6"), cartesian.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1), cartesian.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2), cartesian.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1), cartesian.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 1, 2, 2, 3, 3, 1, 1, 2, 2, 3, 3), cartesian.getColumn(3));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), true, false, true, false, true, false, true, false, true, false, true, false), cartesian.getColumn(4));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), "string1", "string2", "string1", "string2", "string1", "string2", "string1", "string2", "string1", "string2", "string1", "string2"),
                cartesian.getColumn(5));
    }

    @Test
    public void test_cartesian_1()
    {
        TupleVector outer = TupleVector.of(schema("col1"), asList(vv(ResolvedType.of(Type.Any), 0, 1)));
        TupleVector inner = TupleVector.of(schema("col5", "col6"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));

        /*
         * 0 0 0 1 1 1 0 0 1 0 0 1 1 2 3 1 2 3
         * 
         */

        TupleVector cartesian = VectorUtils.cartesian(outer, inner);

        assertEquals(3, cartesian.getSchema()
                .getSize());
        assertEquals(6, cartesian.getRowCount());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 0, 1, 1, 1), cartesian.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 1, 0, 0, 1), cartesian.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 3, 1, 2, 3), cartesian.getColumn(2));
    }

    @Test
    public void test_merge_same()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        assertSame(one, VectorUtils.merge(asList(one)));
    }

    @Test
    public void test_merge()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));

        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(5, actual.getRowCount());
        assertEquals(schema("col1", "col2"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 1, 2, 3), actual.getColumn(1));
    }

    @Test
    public void test_merge_different_vectors()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col2", "col3"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));
        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(5, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 0, 0, 1), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 1, 2, 3), actual.getColumn(2));
    }

    @Test
    public void test_merge_different_vectors_different_type()
    {
        TupleVector one = TupleVector.of(schema(new Type[] { Type.Any, Type.Int }, "col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Int), 1, 2)));
        TupleVector two = TupleVector.of(schema("col2", "col3"), asList(vv(ResolvedType.of(Type.Any), 0, 0, 1), vv(ResolvedType.of(Type.Any), 1, 2, 3)));
        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(5, actual.getRowCount());
        assertEquals(schema(new Type[] { Type.Any, Type.Int, Type.Any, Type.Any }, "col1", "col2", "col2", "col3"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 1, 2, null, null, null), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 1, 2, 3), actual.getColumn(3));
    }

    @Test
    public void test_merge_different_vectors_1()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col3", "col2"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 0, 0, 1)));
        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(3, actual.getSchema()
                .getSize());
        assertEquals(5, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 0, 0, 1), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 1, 2, 3), actual.getColumn(2));
    }

    @Test
    public void test_merge_different_vectors_2()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 0, 1), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 0, 0, 1)));
        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(5, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 1, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, null, null, null), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 1, 2, 3), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, 0, 0, 1), actual.getColumn(3));
    }

    @Test
    public void test_merge_different_vectors_3()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 4, 4), vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 2), vv(ResolvedType.of(Type.Any), 5)));
        TupleVector three = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 6), vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 3)));
        TupleVector actual = VectorUtils.merge(asList(one, two, three));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(4, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 4, 5, 6), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, null, 1), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, null, 3), actual.getColumn(3));
    }

    @Test
    public void test_merge_different_vectors_4()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 4, 4), vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector two = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 6), vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 3)));
        TupleVector three = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 2), vv(ResolvedType.of(Type.Any), 5)));
        TupleVector actual = VectorUtils.merge(asList(one, two, three));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(4, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 0, 0, 1, null), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 2, 3, null), actual.getColumn(3));
    }

    @Test
    public void test_merge_different_vectors_5()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 2), vv(ResolvedType.of(Type.Any), 5)));
        TupleVector two = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 4, 4), vv(ResolvedType.of(Type.Any), 0, 0), vv(ResolvedType.of(Type.Any), 1, 2)));
        TupleVector three = TupleVector.of(schema("col1", "col2", "col3", "col4"),
                asList(vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 6), vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 3)));
        TupleVector actual = VectorUtils.merge(asList(one, two, three));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(4, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 2, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, 4, 4, 6), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 1, 2, 3), actual.getColumn(3));
    }

    @Test
    public void test_merge_different_vectors_6()
    {
        TupleVector one = TupleVector.of(schema("col1", "col2"), asList(vv(ResolvedType.of(Type.Any), 2), vv(ResolvedType.of(Type.Any), 5)));
        TupleVector two = TupleVector.of(schema("col3", "col4"), asList(vv(ResolvedType.of(Type.Any), 1), vv(ResolvedType.of(Type.Any), 3)));
        TupleVector actual = VectorUtils.merge(asList(one, two));

        assertEquals(4, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());
        assertEquals(schema("col1", "col2", "col3", "col4"), actual.getSchema());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 2, null), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, null), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 1), actual.getColumn(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 3), actual.getColumn(3));
    }
}
