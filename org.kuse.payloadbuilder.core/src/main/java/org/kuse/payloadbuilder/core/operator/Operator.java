package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;

import java.util.Iterator;

/** Definition of a selection operator */
public interface Operator extends DescribableNode
{
    /** Empty operator */
    Operator EMPTY_OPERATOR = context -> RowIterator.EMPTY;

    /** Open iterator */
    RowIterator open(ExecutionContext context);

    /**
     * To string with indent. Used when printing operator tree
     *
     * @param indent Indent count
     */
    default String toString(int indent)
    {
        return toString();
    }

    /** Definition of a iterator that stream {@link Tuple}'s */
    public interface RowIterator
    {
        RowIterator EMPTY = wrap(emptyIterator());

        /** Wrap an {@link Iterator} of {@link Tuple} in a {@link RowIterator} */
        static RowIterator wrap(Iterator<Tuple> iterator)
        {
            return wrap(iterator, null);
        }

        /** Wrap an {@link Iterator} of {@link Tuple} in a {@link RowIterator} */
        static RowIterator wrap(Iterator<Tuple> iterator, Runnable close)
        {
            //CSOFF
            return new RowIterator()
            //CSON
            {
                @Override
                public Tuple next()
                {
                    return iterator.next();
                }

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public void close()
                {
                    if (close != null)
                    {
                        close.run();
                    }
                }
            };
        }

        /** Returns true if this iterator has more elements else false */
        boolean hasNext();

        /** Returns next tuple */
        Tuple next();

        /** Close iterator */
        default void close()
        {
        }
    }

    /** Extension of {@link RowIterator} that operates over in memory structures */
    public interface RowList extends RowIterator
    {
        /** These methods should not be implemented */
        @Override
        default boolean hasNext()
        {
            throw new IllegalAccessError("Not implemented");
        }

        @Override
        default Tuple next()
        {
            throw new IllegalAccessError("Not implemented");
        }

        /** Return size of list */
        int size();

        /** Return tuple at provided index */
        Tuple get(int index);
    }
}
