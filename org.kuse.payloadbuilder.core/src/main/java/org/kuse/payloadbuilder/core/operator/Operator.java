package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;

import java.util.Iterator;

/** Definition of a selection operator */
public interface Operator extends DescribableNode
{
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
    public interface RowIterator extends Iterator<Tuple>
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

        /** Close iterator */
        default void close()
        {
        }
    }
}
