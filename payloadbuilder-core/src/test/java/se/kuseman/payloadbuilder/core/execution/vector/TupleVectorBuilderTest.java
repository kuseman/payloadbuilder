package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.BitSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link TupleVectorBuilder} */
public class TupleVectorBuilderTest extends APhysicalPlanTest
{
    @Test
    public void test_append_with_constant_scan_input()
    {
        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);
        b.append(TupleVector.CONSTANT, vv(Type.Boolean, true));

        TupleVector actual = b.build();

        assertEquals(Schema.EMPTY, actual.getSchema());
        assertEquals(1, actual.getRowCount());

        b.append(TupleVector.CONSTANT);

        actual = b.build();
        assertEquals(Schema.EMPTY, actual.getSchema());
        assertEquals(2, actual.getRowCount());
    }

    @Test
    public void test_invalid_filter_size()
    {
        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Long)), asList(
                vv(Type.Int, 1, null, 3),
                vv(Type.Long, 4, 5, 6)));
        //@formatter:on

        try
        {
            b.append(vector, vv(Type.Boolean, true, false));
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Filter size must equal tuple vector row count"));
        }
    }

    @Test
    public void test_filter()
    {
        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Long)), asList(
                vv(Type.Int, 1, null, 3),
                vv(Type.Long, 4, 5, 6)));
        //@formatter:on

        ValueVector filter = vv(Type.Boolean, true, false, true);

        b.append(vector, filter);

        TupleVector actual = b.build();

        //@formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Long)
        );

        assertEquals(expectedSchema, actual.getSchema());

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(expectedSchema, asList(
                vv(Type.Int, 1, 3),
                vv(Type.Long, 4, 6)
                )), actual);
        //@formatter:on
    }

    @Test
    public void test_append_populate()
    {
        Schema innerSchema = Schema.of(Column.of("col2", Type.Float), Column.of("col3", Type.String));

        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);
        //@formatter:off
        TupleVector outer = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Long)), asList(
                vv(Type.Int, 1, 3, 7),
                vv(Type.Long, 4, 5, 10)));
        TupleVector inner = TupleVector.of(innerSchema, asList(
                vv(Type.Float, 4F, 10F, null),
                vv(Type.String, "hello", null, "world")));
        //@formatter:on

        //@formatter:off
        //                                    1        1      1      3        3     3
        //                                    4        4      4      5        5     5
        //                                    4F,      10F,   null   4F,      10F,  null
        //                                    "hello", null, "world" "hello", null, "world"
        ValueVector filter = vv(Type.Boolean, true,    false, true,  false,   true, false, false, false, false);
        //@formatter:on

        b.appendPopulate(outer, inner, filter, "p");

        TupleVector actual = b.build();

        //@formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Long),
                pop("p", ResolvedType.table(innerSchema), null)
        );

        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .isEqualTo(expectedSchema);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(expectedSchema, asList(
                vv(Type.Int, 1, 3),
                vv(Type.Long, 4, 5),
                vv(ResolvedType.table(inner.getSchema()),
                        TupleVector.of(innerSchema, asList(
                                vv(Type.Float, 4F, null),
                                vv(Type.String, "hello", "world")
                                )),
                        TupleVector.of(innerSchema, asList(
                                vv(Type.Float, 10F),
                                vv(Type.String, (String) null)
                                ))
                    )
                )), actual);
        //@formatter:on
    }

    @Test
    public void test_filter_bitset()
    {
        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Long)), asList(
                vv(Type.Int, 1, null, 3),
                vv(Type.Long, 4, 5, 6)));
        //@formatter:on

        BitSet filter = new BitSet();
        filter.set(0);
        filter.set(2);

        b.append(vector, filter);

        TupleVector actual = b.build();

        //@formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Long)
        );

        assertEquals(expectedSchema, actual.getSchema());

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(expectedSchema, asList(
                vv(Type.Int, 1, 3),
                vv(Type.Long, 4, 6)
                )), actual);
        //@formatter:on
    }

    @Test
    public void test()
    {
        TupleVectorBuilder b = new TupleVectorBuilder(new VectorFactory(new BufferAllocator(new BufferAllocator.AllocatorSettings())), 5);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, b.build());

        TupleVector vector = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.Long)), asList(vv(Type.Int, 1, null, 3), vv(Type.Long, 4, 5, 6)));

        b.append(vector);

        VectorTestUtils.assertTupleVectorsEquals(vector, b.build());

        // New column last
        vector = TupleVector.of(Schema.of(Column.of("col2", Type.Long), Column.of("col3", Type.Float)), asList(vv(Type.Long, 10, null, 30), vv(Type.Float, 40, 50, 60)));

        b.append(vector);

        TupleVector actual = b.build();

        //@formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Long),
                Column.of("col3", Type.Float)
                );
        
        assertEquals(expectedSchema, actual.getSchema());
        
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(expectedSchema, asList(
                vv(Type.Int, 1, null, 3, null, null, null),
                vv(Type.Long, 4, 5, 6, 10, null, 30),
                vv(Type.Float, null, null, null, 40, 50, 60)
                )), actual);
        //@formatter:on

        // New column first
        vector = TupleVector.of(Schema.of(Column.of("col4", Type.Boolean)), asList(vv(Type.Boolean, true, false)));

        b.append(vector);

        actual = b.build();

        //@formatter:off
        expectedSchema = Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Long),
                Column.of("col3", Type.Float),
                Column.of("col4", Type.Boolean)
                );

        assertEquals(expectedSchema, actual.getSchema());

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(expectedSchema, asList(
                vv(Type.Int, 1, null, 3, null, null, null, null, null),
                vv(Type.Long, 4, 5, 6, 10, null, 30, null, null),
                vv(Type.Float, null, null, null, 40, 50, 60, null, null),
                vv(Type.Boolean, null, null, null, null, null, null, true, false)
                )), actual);
        //@formatter:on
    }
}
