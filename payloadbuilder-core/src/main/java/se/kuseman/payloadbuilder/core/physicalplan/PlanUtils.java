package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.vector.ChainedTupleVector;
import se.kuseman.payloadbuilder.api.execution.vector.ITupleVectorBuilder;

/** Utils for plans */
public class PlanUtils
{
    private static final int DEFAULT_SIZE = 500;

    /**
     * Chains provided iterator into a single tuple vector. NOTE! Closes the iterator. Requires that all iterated tuple vectors shares a common sub schema.
     */
    static TupleVector chain(IExecutionContext context, TupleIterator iterator)
    {
        try
        {
            int estimatedBatchCount = iterator.estimatedBatchCount();
            List<TupleVector> vectors = new ArrayList<>(estimatedBatchCount > 0 ? estimatedBatchCount
                    : 10);
            while (iterator.hasNext())
            {
                vectors.add(iterator.next());
            }
            return ChainedTupleVector.chain(vectors);
        }
        finally
        {
            iterator.close();
        }
    }

    /** Concats provided iterator into a single tuple vector. NOTE! Closes the iterator */
    public static TupleVector concat(IExecutionContext context, TupleIterator iterator)
    {
        try
        {
            return concat(context, iterator, -1);
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
    static TupleVector concat(IExecutionContext context, TupleIterator iterator, int maxRows)
    {
        ITupleVectorBuilder b = null;
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

                b = context.getVectorFactory()
                        .getTupleVectorBuilder(estimatedSize);
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
