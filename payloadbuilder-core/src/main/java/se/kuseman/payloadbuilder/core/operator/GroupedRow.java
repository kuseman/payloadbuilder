package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.operator.Operator.TupleList;
import se.kuseman.payloadbuilder.api.operator.Tuple;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple, IGroupedRow
{
    private static final IntSet EMPTY = IntSets.unmodifiable(new IntOpenHashSet());

    private final int tupleOrdinal;
    private final List<Tuple> tuples;
    /** Map of tuple ordinals the it's grouped columns */
    private final Int2ObjectMap<GroupedOrdinalRow> ordinalRows;
    /** Column ordinals of "this" tuple ordial */
    private final IntSet currentOrdinals;

    GroupedRow(List<Tuple> tuples, Int2ObjectMap<IntSet> columnOrdinals)
    {
        if (isEmpty(tuples))
        {
            throw new IllegalArgumentException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        // Ordinal of a group by is the same as the rows
        this.tupleOrdinal = tuples.get(0)
                .getTupleOrdinal();
        this.ordinalRows = getOrdinalRows(requireNonNull(columnOrdinals));
        IntSet set = columnOrdinals.get(tupleOrdinal);
        this.currentOrdinals = set == null ? EMPTY
                : set;
    }

    private Int2ObjectMap<GroupedOrdinalRow> getOrdinalRows(Int2ObjectMap<IntSet> columnOrdinals)
    {
        Int2ObjectMap<GroupedOrdinalRow> result = new Int2ObjectOpenHashMap<>(columnOrdinals.size());
        Iterator<Entry<IntSet>> it = columnOrdinals.int2ObjectEntrySet()
                .iterator();
        while (it.hasNext())
        {
            Entry<IntSet> entry = it.next();
            result.put(entry.getIntKey(), new GroupedOrdinalRow(entry.getIntKey(), entry.getValue()));
        }
        return result;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public int getColumnCount()
    {
        return tuples.get(0)
                .getColumnCount();
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return tuples.get(0)
                .getColumnOrdinal(column);
    }

    @Override
    public String getColumn(int columnOrdinal)
    {
        return tuples.get(0)
                .getColumn(columnOrdinal);
    }

    @Override
    public Tuple getTuple(int tupleOrdinal)
    {
        if (tupleOrdinal == this.tupleOrdinal)
        {
            return this;
        }

        // Grouped ordinal return ordinal row
        GroupedOrdinalRow ordinalRow = ordinalRows.get(tupleOrdinal);
        if (ordinalRow == null)
        {
            // Create a new grouped ordinal row for the target
            // The grouped columns tuple ordinals are pre created in constructor since
            // these are likely to be accessed
            ordinalRow = new GroupedOrdinalRow(tupleOrdinal, null);
            ordinalRows.put(tupleOrdinal, ordinalRow);
        }
        return ordinalRow;
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        if (currentOrdinals.contains(columnOrdinal))
        {
            return tuples.get(0)
                    .getValue(columnOrdinal);
        }

        return new Iterator<Object>()
        {
            int index;

            @Override
            public boolean hasNext()
            {
                return index < tuples.size();
            }

            @Override
            public Object next()
            {
                return tuples.get(index++)
                        .getValue(columnOrdinal);
            }
        };
    }

    @Override
    public List<Tuple> getContainedRows()
    {
        return tuples;
    }

    /**
     * <pre>
     * A grouped row targeted to a specific tuple ordinal.
     * If the grouped columns is present in the target tuple ordinal
     * values from these are returned as single values
     * </pre>
     */
    private class GroupedOrdinalRow extends AbstractList<Tuple> implements Tuple, TupleList
    {
        private final int rowTupleOrdinal;
        private final IntSet columnOrdinals;

        private GroupedOrdinalRow(int tupleOrdinal, IntSet columnOrdinals)
        {
            this.rowTupleOrdinal = tupleOrdinal;
            this.columnOrdinals = columnOrdinals;
        }

        /* TupleList implementation */

        @Override
        public Tuple get(int index)
        {
            return tuples.get(index)
                    .getTuple(rowTupleOrdinal);
        }

        @Override
        public int size()
        {
            return tuples.size();
        }

        /* TupleList implementation */

        @Override
        public int getTupleOrdinal()
        {
            return rowTupleOrdinal;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            Tuple t = tuples.get(0)
                    .getTuple(rowTupleOrdinal);
            return t != null ? t.getColumnOrdinal(column)
                    : -1;
        }

        @Override
        public int getColumnCount()
        {
            Tuple t = tuples.get(0)
                    .getTuple(rowTupleOrdinal);
            return t != null ? t.getColumnCount()
                    : 0;
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            Tuple t = tuples.get(0)
                    .getTuple(rowTupleOrdinal);
            return t != null ? t.getColumn(columnOrdinal)
                    : null;
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            if (columnOrdinals != null
                    && columnOrdinals.contains(columnOrdinal))
            {
                Tuple t = tuples.get(0)
                        .getTuple(rowTupleOrdinal);
                return t != null ? t.getValue(columnOrdinal)
                        : null;
            }

            return new Iterator<Object>()
            {
                int index;

                @Override
                public boolean hasNext()
                {
                    return index < tuples.size();
                }

                @Override
                public Object next()
                {
                    Tuple t = tuples.get(index++)
                            .getTuple(rowTupleOrdinal);
                    return t != null ? t.getValue(columnOrdinal)
                            : null;
                }
            };
        }
    }
}
