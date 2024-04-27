package se.kuseman.payloadbuilder.api.execution.vector;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link ChainedTupleVector} */
public class ChainedTupleVectorTest extends Assert
{
    @Test
    public void test_no_common_schema_sub_Set()
    {
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, ChainedTupleVector.chain(List.of()));

        //@formatter:off
        TupleVector tv1 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        
        TupleVector tv2 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Float),
                Column.of("col2", Type.Boolean)),
                List.of(
                    VectorTestUtils.vv(Type.Float, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        //@formatter:on

        try
        {
            ChainedTupleVector.chain(List.of(tv1, tv2));
            fail("Should fail cause of failed chain");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Schema of chained tuple vectors must share a common sub set of columns."));
        }
    }

    @Test
    public void test_no_or_single_vectors()
    {
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, ChainedTupleVector.chain(List.of()));

        //@formatter:off
        TupleVector tv1 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        //@formatter:on

        assertSame(tv1, ChainedTupleVector.chain(List.of(tv1)));
    }

    @Test
    public void test_chain_same_schema_vectors()
    {
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, ChainedTupleVector.chain(List.of()));

        //@formatter:off
        TupleVector tv1 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        
        TupleVector tv2 = TupleVector.of(tv1.getSchema(),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        //@formatter:on

        assertSame(tv1.getSchema(), ChainedTupleVector.chain(List.of(tv1, tv2))
                .getSchema());
    }

    @Test
    public void test()
    {
        //@formatter:off
        TupleVector tv1 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3),
                    VectorTestUtils.vv(Type.Boolean, null, true, false)));
        
        TupleVector tv2 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 10, null, 30)));
        
        TupleVector tv3 = TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean),
                Column.of("col3", Type.String)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 100, null),
                    VectorTestUtils.vv(Type.Boolean, true, false),
                    VectorTestUtils.vv(Type.String, "hello", "world")));

        TupleVector actual = ChainedTupleVector.chain(List.of(tv1, tv2, tv3));
        
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean),
                Column.of("col3", Type.String)),
                List.of(
                    VectorTestUtils.vv(Type.Int, 1, 2, 3, 10, null, 30, 100, null),
                    VectorTestUtils.vv(Type.Boolean, null, true, false, null, null, null, true, false),
                    VectorTestUtils.vv(Type.String, null, null, null, null, null, null, "hello", "world")
                )), actual);
        //@formatter:on

        // Test some random access
        assertEquals(1, actual.getColumn(0)
                .getInt(0));

        assertEquals(UTF8String.from("world"), actual.getColumn(2)
                .getString(7));
    }

    @Test
    public void test_all_get_methods_are_declared()
    {
        List<String> methods = Arrays.stream(Column.Type.values())
                .map(t -> "get" + t)
                .collect(toList());

        for (String method : methods)
        {
            try
            {
                ChainedTupleVector.ChainedValueVector.class.getDeclaredMethod(method, int.class);
            }
            catch (NoSuchMethodException e)
            {
                fail(ChainedTupleVector.ChainedValueVector.class.getSimpleName() + " should have method: " + method);
            }
        }
    }
}
