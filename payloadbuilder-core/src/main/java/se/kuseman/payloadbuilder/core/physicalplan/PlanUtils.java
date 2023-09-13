package se.kuseman.payloadbuilder.core.physicalplan;

import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;

/** Utils for plans */
class PlanUtils
{
    private static final int DEFAULT_SIZE = 500;

    /** Concats provided iterator into a single tuple vector. NOTE! Closes the iterator */
    static TupleVector concat(BufferAllocator allocator, TupleIterator iterator)
    {
        try
        {
            return concat(allocator, iterator, -1);
        }
        finally
        {
            iterator.close();
        }
    }

    /**
     * Concats provided iterator into a single tuple vector. Collects rows until maxRows are reached. NOTE! Resulting vector might overflow maxRows since this method doesn't split any vectors. Use -1
     * for concating whole iterator
     */
    static TupleVector concat(BufferAllocator allocator, TupleIterator iterator, int maxRows)
    {
        TupleVectorBuilder b = null;
        TupleVector vector = null;

        int rowCount = 0;
        while (iterator.hasNext())
        {
            TupleVector v = iterator.next();
            rowCount += v.getRowCount();

            if (vector == null)
            {
                vector = v;
            }
            else if (b == null)
            {
                int estimatedSize = 0;
                int estimatedBatchCount = iterator.estimatedBatchCount();

                int actualMaxRows = maxRows > 0 ? Math.min(maxRows, DEFAULT_SIZE)
                        : DEFAULT_SIZE;

                if (estimatedBatchCount > 0)
                {
                    // Allocate the minimum needed for this iterator
                    estimatedSize = Math.min(actualMaxRows, Math.max(v.getRowCount(), vector.getRowCount()) * estimatedBatchCount);
                }
                else
                {
                    estimatedSize = actualMaxRows;
                }

                b = new TupleVectorBuilder(allocator, estimatedSize);
                b.append(vector);
                b.append(v);
            }
            else
            {
                b.append(v);
            }

            if (maxRows > 0
                    && rowCount >= maxRows)
            {
                break;
            }
        }

        if (b != null)
        {
            return b.build();
        }
        else if (vector != null)
        {
            return vector;
        }
        return TupleVector.EMPTY;
    }
}
