package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link PlanUtils} */
class PlanUtilsTest extends APhysicalPlanTest
{
    @Test
    void test_no_allocations()
    {
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col", Type.Int)), asList(ValueVector.literalInt(100, 500)));
        TupleVector actual = PlanUtils.concat(context, TupleIterator.singleton(vector));

        assertEquals(500, actual.getRowCount());

        // Only one vector => no allocations
        assertEquals(0, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationSum());
        assertEquals(0, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationCount());

    }

    @Test
    void test_full_batching()
    {
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col", Type.Int)), asList(range(500)));
        TupleVector actual = PlanUtils.concat(context, split(vector, -1));

        VectorTestUtils.assertVectorsEquals(range(500), actual.getColumn(0));

        // 1 allocation of 500 ints
        assertEquals(500, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationSum());
        assertEquals(1, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationCount());
    }

    @Test
    void test_batching_with_estimated_batch_count()
    {
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col", Type.Int)), asList(range(500)));
        TupleIterator it = split(vector, vector.getRowCount());

        TupleVector actual = PlanUtils.concat(context, it, 250);
        assertTrue(it.hasNext());

        VectorTestUtils.assertVectorsEquals(range(250), actual.getColumn(0));

        // 1 allocation of 250 ints
        assertEquals(250, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationSum());
        assertEquals(1, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationCount());
    }

    @Test
    void test_batching_with_no_estimated_batch_count()
    {
        TupleVector vector = TupleVector.of(Schema.of(Column.of("col", Type.Int)), asList(range(500)));
        TupleIterator it = split(vector, -1);

        TupleVector actual = PlanUtils.concat(context, it, 250);
        assertTrue(it.hasNext());

        VectorTestUtils.assertVectorsEquals(range(250), actual.getColumn(0));

        // 1 allocation of 250 ints
        assertEquals(250, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationSum());
        assertEquals(1, context.getBufferAllocator()
                .getStatistics()
                .getIntAllocationCount());
    }

    private TupleIterator split(TupleVector vector, int estimatedBatchCount)
    {
        return new TupleIterator()
        {
            private TupleVector next;
            private int vectorRow = 0;

            @Override
            public int estimatedBatchCount()
            {
                return estimatedBatchCount;
            }

            @Override
            public TupleVector next()
            {
                TupleVector result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                if (next != null)
                {
                    return true;
                }
                else if (vectorRow >= vector.getRowCount())
                {
                    return false;
                }

                final int currentRow = vectorRow;

                next = new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return vector.getSchema();
                    }

                    @Override
                    public int getRowCount()
                    {
                        return 1;
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        return new ValueVectorAdapter(vector.getColumn(column))
                        {
                            @Override
                            protected int getRow(int row)
                            {
                                return currentRow;
                            }
                        };
                    }
                };
                vectorRow++;
                return true;
            }
        };
    }

    private ValueVector range(int size)
    {
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return row;
            }
        };
    }
}
