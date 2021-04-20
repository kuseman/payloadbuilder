package org.kuse.payloadbuilder.core.operator;

import java.util.Arrays;
import java.util.Objects;

/**
 * Tuple that is composed of other tuples
 *
 * <pre>
 *  For a join like this:
 *
 *  select *
 *  from tableA a
 *  inner join tableB b
 *      on b.col = a.col
 *
 *  We will get a stream of CompositeTuple's like this:
 *
 *  CompositeTuple
 *    Row(a)
 *    Row(b)
 * </pre>
 */
class CompositeTuple implements Tuple
{
    private final int tupleOrdinal;
    final Tuple[] tuples;
    private int index;

    CompositeTuple(Tuple outer, Tuple inner, int capacity)
    {
        this.tuples = new Tuple[capacity];
        add(outer);
        add(inner);

        // A compsite's ordinal is always the outer tuples ordinal minus one
        // this to keep the hierarchy tree intact regarding a tulpes ordinal
        // should always be lower that all it's descendants
        this.tupleOrdinal = outer.getTupleOrdinal() - 1;
    }

    /** Optimized tuple ctor */
    private CompositeTuple(int tupleOrdinal, Tuple[] tuples)
    {
        this.tupleOrdinal = tupleOrdinal;
        this.tuples = tuples;
    }

    /**
     * Add provided tuple to this composite tuple
     *
     * @param tupleOrdinal The tuple ordinal slot that we should add provided tuple to. If that slot is taken a copy is returned
     */
    CompositeTuple add(Tuple tuple)
    {
        CompositeTuple result = this;
        // Unwrap tuple if composite so we end up with a flat structure
        if (tuple instanceof CompositeTuple)
        {
            /*
             * select *
             * from tableA a                0
             * inner join                   1
             * (
             *   select *
             *   from tableB b              2
             *   inner join tableC c        3
             *     on ...
             * ) b
             *  on ...
             * inner join                   4
             * (
             *   select *
             *   from tableD d              5
             *   inner join tableE e        6
             *     on ...
             * ) c
             *  on ...
             *
             *
             * this tuples
             *  row a       (tuple 1)
             *  row b       (tuple 2 starts here)
             *  row c
             *  null        (tuple 3 starts here)
             *  null
             *
             * tuple 1 (ordinal 0)  (row)
             *  row a  (ordinal 0)
             *
             * tuple 2 (ordinal 1)  (sub query)
             *  row b  (ordinal 2)
             *  row c  (ordinal 3)
             *
             * tuple 3 (ordinal 4)  (sub query)
             *  row d  (ordinal 5)
             *  row e  (ordinal 6)
             *
             * If the ordinal in this is occupied we need to make a copy
             * and overwrite tuples starting at the ordinal index
             *
             */
            CompositeTuple ci = (CompositeTuple) tuple;

            /*
             *  checkIndex: current index minus input tuple size
             *
             *  ie. index = 3 (tuple 1 and 2 added)
             *  input tuple is tuple 1 which has length of 2 (we are about to add a tuple that should trigger a copy)
             *
             *  checkIndex = 3 - 2 => 1
             *  if tuple at index = 1 has the same ordinal as the first tuple we are about to add, make a copy
             */

            int checkIndex = index - ci.tuples.length;
            Tuple tupleToCheck = checkIndex >= 0 ? tuples[checkIndex] : null;

            // We already have tuples with target ordinal at position so make a copy and alter the insert index
            if (tupleToCheck != null && tupleToCheck.getTupleOrdinal() == ci.tuples[0].getTupleOrdinal())
            {
                result = new CompositeTuple(tupleOrdinal, Arrays.copyOf(tuples, tuples.length));
                result.index = checkIndex;
            }

            for (int i = 0; i < ci.tuples.length; i++)
            {
                if (ci.tuples[i] != null)
                {
                    result.tuples[result.index++] = ci.tuples[i];
                }
            }
        }
        else
        {
            // If we are about to add a tuple in a taken slot
            // then make a copy and replace the last tuple
            if (index > 0 && tuples[index - 1].getTupleOrdinal() == tuple.getTupleOrdinal())
            {
                CompositeTuple newTuple = new CompositeTuple(tupleOrdinal, Arrays.copyOf(tuples, tuples.length));
                newTuple.tuples[index - 1] = tuple;
                newTuple.index = index;
                return newTuple;
            }
            tuples[index++] = tuple;
        }
        return result;
    }

    /**
     * <pre>
     * Returns tuple for provided ordinal in this tuple,
     * used when having populated joins to find the collection tuple to append result to
     * </pre>
     */
    Tuple getCollectionTupleForMerge(int tupleOrdinal)
    {
        int length = tuples.length;
        for (int i = 0; i < length; i++)
        {
            Tuple tuple = tuples[i];
            if (tuple != null
                && tuple.getTupleOrdinal() == tupleOrdinal)
            {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public int getColumnCount()
    {
        return tuples[0].getColumnCount();
    }

    @Override
    public Tuple getTuple(int tupleOrdinal)
    {
        /*
         * Return tuple with provided ordinal
         *
         * Ex. composite layout
         *
         * tableA           ordinal 0
         * subQeury         ordinal 1
         *   tableB           ordinal 2
         *   tableC           ordinal 3
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

        // Tuple access on a composite, return the first tuple
        if (tupleOrdinal == -1)
        {
            return tuples[0];
        }

        // Start from bottom to see if we should delegate
        int size = tuples.length;
        Tuple prevTuple = null;
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = tuples[i];
            if (tuple == null)
            {
                continue;
            }
            int ordinal = tuple.getTupleOrdinal();
            if (ordinal == tupleOrdinal)
            {
                return tuple;
            }
            // If we passed the ordinal wanted, it's a child of previous tuple
            else if (ordinal > tupleOrdinal
                && prevTuple != null)
            {
                return prevTuple.getTuple(tupleOrdinal);
            }
            prevTuple = tuple;
        }

        // If we came here the wanted tuple ordinal doesn't exits or is a child of
        // the last tuple, delegate
        return prevTuple.getTuple(tupleOrdinal);
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        return tuples[0].getValue(columnOrdinal);
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return tuples[0].getColumnOrdinal(column);
    }

    @Override
    public String getColumn(int columnOrdinal)
    {
        return tuples[0].getColumn(columnOrdinal);
    }

    @Override
    public Tuple optimize(ExecutionContext context)
    {
        int size = tuples.length;
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = tuples[i];
            if (tuple == null)
            {
                continue;
            }

            Tuple child;
            //CSOFF
            if (tuple instanceof CollectionTuple && (child = ((CollectionTuple) tuple).getSingleTuple()) != null)
            //CSON
            {
                tuple = child;
            }

            tuples[i] = tuple.optimize(context);
        }

        return new CompositeTuple(tupleOrdinal, Arrays.stream(tuples).filter(Objects::nonNull).toArray(Tuple[]::new));
    }
}
