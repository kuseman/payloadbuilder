package org.kuse.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Tuple that is composed of other tuples */
public class CompositeTuple extends ArrayList<Tuple> implements Tuple
{
    private final int tupleOrdinal;

    public CompositeTuple(Tuple outer, Tuple inner, int capacity)
    {
        super(capacity);
        add(outer);

        // Unwrap inner to get a flat structure
        if (inner instanceof CompositeTuple)
        {
            addAll((CompositeTuple) inner);
        }
        else
        {
            add(inner);
        }
        // A compsite's ordinal is always the outer tuples ordinal minus one
        // this to keep the hierarchy tree intact regaring a tulpes ordinal
        // should always be lower that all it's descendants
        this.tupleOrdinal = outer.getTupleOrdinal() - 1;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        /*
         * Return tuple with provided ordinal
         *
         * Ex. composite layout
         *
         * tableA           ordinal 0
         * subQeury         ordinal 1
         *   tableB         ordinal 2
         *   tableC         ordinal 3
         * tableD           ordinal 4
         *
         * Wanted ordinal is 3 that isn't located in this tuple
         * but in one of it's children
         */

        /*
         * select x.id     <- ordinal 2
         * ,      y.id     <- ordinal 4
         * from tableA
         * inner join
         * () x with (pop)
         *   on ...
         * inner join
         * () y with (pop)
         *   on ...
         *
         * CompositeTuple
         *   Row (a)            or = 0
         *   Collection (x)     or = 1
         *   Collection (y)     or = 3
         */

        // Start from bottom to see if we should delegate
        int size = size();
        Tuple prevTuple = null;
        int prevTupleOrdinal = Integer.MAX_VALUE;
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = get(i);
            int tupleOrdinal = tuple.getTupleOrdinal();
            if (ordinal == tupleOrdinal)
            {
                return tuple;
            }
            // If we passed the ordinal wanted, it's a child of previous tuple
            else if (tupleOrdinal > ordinal
                && prevTupleOrdinal < ordinal)
            {
                return prevTuple.getTuple(ordinal);
            }

            prevTuple = tuple;
            prevTupleOrdinal = tupleOrdinal;
        }

        // If we came here the wanted tuple ordinal doesn't exits or is a child of
        // the last tuple, delegate
        return prevTuple.getTuple(ordinal);
    }

    @Override
    public Object getValue(int ordinal)
    {
        // Column access on a collection, delegate to first child
        return get(0).getValue(ordinal);
    }

    @Override
    public Object getValue(String column)
    {
        // Column access on a composite, delegate to first child
        return get(0).getValue(column);
    }

    @Override
    public Iterator<TupleColumn> getColumns(int tupleOrdinal)
    {
        final int size = size();
        //CSOFF
        return new Iterator<TupleColumn>()
        //CSON
        {
            int index;
            Iterator<TupleColumn> current;
            TupleColumn next;

            @Override
            public TupleColumn next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleColumn result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (current == null)
                    {
                        if (index >= size)
                        {
                            return false;
                        }

                        Tuple tuple = get(index++);
                        // Process all tuples
                        if (tupleOrdinal == -1
                            ||
                        // A specific tuple ordinal wanted
                            (tupleOrdinal >= 0
                                &&
                        /* The ordinal wanted is pointing to the composite tuples ordinal
                         * This happens for example when an asterisk select is targeted a sub query
                         * Then we process all descendant tuples
                         * ie
                         * select x.*
                         * from
                         * (
                         *    select
                         *    from ....
                         * ) x
                         *
                         */
                                ((tupleOrdinal == CompositeTuple.this.tupleOrdinal
                                    // All descendant ordinals are larger than the parents
                                    && tupleOrdinal < tuple.getTupleOrdinal())
                                    ||
                        // A tuple ordinal match
                                    tupleOrdinal == tuple.getTupleOrdinal())))
                        {
                            current = tuple.getColumns(tupleOrdinal);
                        }
                    }

                    if (current == null || !current.hasNext())
                    {
                        current = null;
                        continue;
                    }
                    next = current.next();
                }
                return true;
            }
        };
    }
}
