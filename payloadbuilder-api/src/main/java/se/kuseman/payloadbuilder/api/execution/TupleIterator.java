package se.kuseman.payloadbuilder.api.execution;

import java.util.Iterator;
import java.util.NoSuchElementException;

/** Iterator that streams {@link TupleVector}'s */
public interface TupleIterator extends Iterator<TupleVector>
{
    TupleIterator EMPTY = new TupleIterator()
    {
        @Override
        public TupleVector next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }
    };

    /**
     * <pre>
     * Returns true if this iterator may block on the next hasNext() call
     * waiting for data that hasn't arrived yet. Default is false (non-blocking).
     * Streaming datasources should override this to return true when
     * all currently available data has been returned.
     * </pre>
     */
    default boolean isBlocking()
    {
        return false;
    }

    /** Close the iterator */
    default void close()
    {
    }

    /** Return an estimate of the number of batches in this iterator. */
    default int estimatedBatchCount()
    {
        return -1;
    }

    /** Return an estimate of the number of rows (total) in this iterator. */
    default int estimatedRowCount()
    {
        return -1;
    }

    /** Creates a singleton iterator from provided vector */
    static TupleIterator singleton(final TupleVector vector)
    {
        return new TupleIterator()
        {
            private boolean done = false;

            @Override
            public int estimatedRowCount()
            {
                return vector.getRowCount();
            }

            @Override
            public int estimatedBatchCount()
            {
                return 1;
            }

            @Override
            public TupleVector next()
            {
                if (done)
                {
                    throw new NoSuchElementException();
                }

                done = true;
                return vector;
            }

            @Override
            public boolean hasNext()
            {
                return !done;
            }
        };
    }
}