package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Definition of a selection operator */
public interface Operator
{
    /** Open iterator */
    RowIterator open(ExecutionContext context);

    /** Get node id of operator */
    int getNodeId();

    /** Return child operators if any */
    default List<Operator> getChildOperators()
    {
        return emptyList();
    }

    /** Returns name of operator when used in describe/analyze statements etc. */
    default String getName()
    {
        return getClass().getSimpleName();
    }

    /**
     * Returns a short describe string of the operator. Used in describe statement
     */
    default String getDescribeString()
    {
        return "";
    }

    /**
     * Returns more detail properties of describe statement if {@link #getDescribeString()} is not enough.
     *
     * @param context Execution context
     */
    default Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return emptyMap();
    }

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
