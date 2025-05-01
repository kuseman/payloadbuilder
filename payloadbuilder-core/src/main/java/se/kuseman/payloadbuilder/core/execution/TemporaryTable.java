package se.kuseman.payloadbuilder.core.execution;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.SelectedValueVector;

import it.unimi.dsi.fastutil.Hash.Strategy;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

/** Holder for temporary table data */
public class TemporaryTable
{
    private static final HashStrategy STRATEGY = new HashStrategy();
    private final TupleVector vector;
    private final List<Index> indices;
    private final Map<Index, IndicesData> indicesData;

    public TemporaryTable(TupleVector vector, List<Index> indices)
    {
        this.vector = requireNonNull(vector, "vector");
        this.indices = requireNonNull(indices, "indices");
        this.indicesData = index();
    }

    /** Return the temporary tables tuple vector */
    public TupleVector getTupleVector()
    {
        return vector;
    }

    public List<Index> getIndices()
    {
        return indices;
    }

    /** Return a tuple iterator from this temporary table by provided index */
    public TupleIterator getIndexIterator(IExecutionContext context, ISeekPredicate seekPredicate)
    {
        if (indices.isEmpty())
        {
            throw new IllegalArgumentException("This tempoarary table does not have any indices");
        }
        final IndicesData data = indicesData.get(seekPredicate.getIndex());

        if (data == null)
        {
            throw new IllegalArgumentException("This tempoarary table does not have index: " + seekPredicate.getIndex());
        }
        final Object2ObjectMap<IndexKey, IntList> table = data.map;

        List<ISeekKey> seekKeys = seekPredicate.getSeekKeys(context);
        int size = seekKeys.size();
        final ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = seekKeys.get(i)
                    .getValue();
        }

        final int valuesSzie = vectors[0].size();
        final int rowCount = valuesSzie * data.averageRowCount;
        final Column.Type[] types = data.columnTypes;

        return new TupleIterator()
        {
            int index;
            TupleVector next;

            @Override
            public int estimatedBatchCount()
            {
                return valuesSzie;
            }

            @Override
            public int estimatedRowCount()
            {
                return rowCount;
            }

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector result = next;
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
                    if (index >= valuesSzie)
                    {
                        return false;
                    }

                    IntList list = table.get(new IndexKey(vectors, index++, types));
                    if (list == null)
                    {
                        continue;
                    }

                    next = new IndexTupleVector(vector, list);
                }
                return true;
            }
        };
    }

    private Map<Index, IndicesData> index()
    {
        if (indices.isEmpty())
        {
            return emptyMap();
        }

        int rowCount = vector.getRowCount();
        int size = indices.size();
        Map<Index, IndicesData> result = new HashMap<>(size);

        for (int i = 0; i < size; i++)
        {
            Index currentIndex = indices.get(i);

            if (currentIndex.getColumnsType() != ColumnsType.ALL)
            {
                throw new IllegalArgumentException("Temporary table indices must have " + ColumnsType.ALL + " columns type");
            }

            Object2ObjectMap<IndexKey, IntList> table = new Object2ObjectOpenCustomHashMap<IndexKey, IntList>(STRATEGY);

            int[] ordinals = rowCount > 0 ? getOrdinals(currentIndex)
                    : ArrayUtils.EMPTY_INT_ARRAY;
            int ordinalsSize = ordinals.length;
            ValueVector[] vectors = new ValueVector[ordinalsSize];
            Column.Type[] types = new Column.Type[ordinalsSize];
            for (int j = 0; j < ordinalsSize; j++)
            {
                ValueVector column = vector.getColumn(ordinals[j]);
                vectors[j] = column;
                types[j] = column.type()
                        .getType();
            }
            // Index all rows for current index
            for (int j = 0; j < rowCount; j++)
            {
                table.computeIfAbsent(new IndexKey(vectors, j, null), k -> new IntArrayList())
                        .add(j);
            }

            int total = 0;
            for (IntList l : table.values())
            {
                total += l != null ? l.size()
                        : 0;
            }
            int averageRowCount = table.size() > 0 ? total / table.size()
                    : 0;

            result.put(currentIndex, new IndicesData(table, averageRowCount, types));
        }

        return result;
    }

    private int[] getOrdinals(Index index)
    {
        Schema schema = vector.getSchema();
        int schemaSize = schema.getSize();
        int size = index.getColumns()
                .size();
        int[] result = new int[size];

        for (int i = 0; i < size; i++)
        {
            String indexColumn = index.getColumns()
                    .get(i);
            result[i] = -1;
            for (int j = 0; j < schemaSize; j++)
            {
                if (schema.getColumns()
                        .get(j)
                        .getName()
                        .equalsIgnoreCase(indexColumn))
                {
                    result[i] = j;
                    break;
                }
            }
            if (result[i] == -1)
            {
                throw new IllegalArgumentException("Cannot index temporary table,  missing column: " + indexColumn + " in schema: " + schema);
            }
        }
        return result;
    }

    /** Tuple vector that wraps temporary table with an indexed list of rows */
    static class IndexTupleVector implements TupleVector
    {
        final TupleVector vector;
        final ValueVector selection;

        IndexTupleVector(TupleVector vector, IntList rows)
        {
            this.vector = vector;
            this.selection = VectorUtils.convertToSelectionVector(rows);
        }

        @Override
        public int getRowCount()
        {
            return selection.size();
        }

        @Override
        public Schema getSchema()
        {
            return vector.getSchema();
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return SelectedValueVector.select(vector.getColumn(column), selection);
        }
    }

    /** Strategy for hash table */
    static class HashStrategy implements Strategy<IndexKey>
    {
        @Override
        public int hashCode(IndexKey o)
        {
            if (o == null)
            {
                return 0;
            }
            return VectorUtils.hash(o.vectors, o.types, o.index);
        }

        @Override
        public boolean equals(IndexKey a, IndexKey b)
        {
            return a != null
                    && b != null
                    && VectorUtils.equals(a.vectors, b.vectors, a.index, b.index);
        }
    }

    /** Key definition to use in hash table */
    static class IndexKey
    {
        final ValueVector[] vectors;
        final int index;
        final Column.Type[] types;

        IndexKey(ValueVector[] vectors, int index, Column.Type[] types)
        {
            this.vectors = vectors;
            this.index = index;
            this.types = ArrayUtils.isEmpty(types) ? null
                    : types;
        }
    }

    /**
     * Indices data with data and statistics.
     *
     * @param map Map with data
     * @param averageRowCount The average row count for index entries in map. Is used to estimate the number of rows an index iterator will return.
     * @param columnTypes The types that the index was hashed with to get properly hash keys when seeking.
     */
    private record IndicesData(Object2ObjectMap<IndexKey, IntList> map, int averageRowCount, Column.Type[] columnTypes)
    {
    }
}
