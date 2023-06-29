package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntPredicate;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Builder that builds a tuple vector by appending one to many tuple vectors and concatenating their value vectors
 */
public class TupleVectorBuilder
{
    private static final Int2IntMap EMPTY = new Int2IntOpenHashMap(0);

    private final BufferAllocator allocator;
    private final int estimatedRowCount;

    /** Resulting builders */
    private List<ABufferVectorBuilder> builders;
    /** Current inner columns */
    private List<Column> columns;
    /** Resulting row count to tuple vector */
    private int rowCount;

    public TupleVectorBuilder(BufferAllocator allocator, int estimatedRowCount)
    {
        this.allocator = requireNonNull(allocator, "allocator");
        this.estimatedRowCount = estimatedRowCount;
    }

    /** Append a populated result from provided cartesian and filter */
    public void appendPopulate(TupleVector cartesian, ValueVector filter, TupleVector outer, TupleVector inner, String populateAlias)
    {
        final int outerSize = outer.getSchema()
                .getSize();
        int outerRowCount = outer.getRowCount();
        int innerRowCount = inner.getRowCount();

        final Schema schema = SchemaUtils.populate(outer.getSchema(), populateAlias, inner.getSchema());
        for (int i = 0; i < outerRowCount; i++)
        {
            int filterStart = i * innerRowCount;
            int filterEnd = filterStart + innerRowCount;

            // TODO: could cache bitset for matched inner rows and reuse created inner vector
            TupleVectorBuilder b = new TupleVectorBuilder(allocator, innerRowCount);
            b.appendInternal(inner, filterStart, filterEnd, row -> filter.getPredicateBoolean(row));

            TupleVector innerResult = b.build();

            // No matches for current outer row, move on to next chunk
            if (innerResult.getRowCount() == 0)
            {
                continue;
            }

            final ValueVector innerColumn = ValueVector.literalTable(innerResult, 1);
            final int outerRow = i;
            TupleVector currentOuterResult = new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    return schema;
                }

                @Override
                public int getRowCount()
                {
                    return 1;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    if (column < outerSize)
                    {
                        return new ValueVectorAdapter(outer.getColumn(column))
                        {
                            @Override
                            protected int getRow(int row)
                            {
                                return outerRow;
                            };
                        };
                    }
                    return innerColumn;
                }
            };
            append(currentOuterResult);
        }
    }

    /** Append a vector to builder, appending it's vectors to buffers */
    public void append(TupleVector vector)
    {
        append(vector, 0, vector.getRowCount());
    }

    /** Append a vector appending it's vectors to buffers using provided filter */
    public void append(TupleVector vector, ValueVector filter)
    {
        int size = filter.size();
        if (vector.getRowCount() != size)
        {
            throw new IllegalArgumentException("Filter size must equal tuple vector row count");
        }
        appendInternal(vector, 0, size, row -> filter.getPredicateBoolean(row));
    }

    /** Append a vector appending it's vectors to buffers using provided filter (as bit set) */
    public void append(TupleVector vector, BitSet filter)
    {
        appendInternal(vector, 0, vector.getRowCount(), row -> filter.get(row));
    }

    /** Append a new vector to builder, appending it's vectors to buffers */
    private void append(TupleVector vector, int from, int to)
    {
        if (builders == null)
        {
            builders = new ArrayList<>(vector.getSchema()
                    .getSize());
            columns = new ArrayList<>(builders.size());
        }

        Int2IntMap mapping = verifySchema(allocator, estimatedRowCount, vector, columns, builders);

        int length = to - from;
        rowCount += length;
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            ABufferVectorBuilder builder = builders.get(i);
            builder.ensureSize(length);

            int columnIndex = mapping.isEmpty() ? i
                    : mapping.get(i);
            // Append null
            if (columnIndex < 0)
            {
                appendNull(builder, length);
            }
            else
            {
                ValueVector v = vector.getColumn(columnIndex);
                for (int j = from; j < to; j++)
                {
                    boolean isNull = v.isNull(j);
                    builder.put(isNull, v, j, 1);
                    builder.putNulls(isNull, 1);
                }
                builder.size += length;
            }
        }
    }

    /** Build resulting tuple vector */
    public TupleVector build()
    {
        if (builders == null)
        {
            return TupleVector.EMPTY;
        }

        int size = builders.size();

        // This happens if appended tuple vectors with no columns
        if (size == 0)
        {
            if (rowCount == 1)
            {
                return TupleVector.CONSTANT;
            }
            return new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    return Schema.EMPTY;
                }

                @Override
                public int getRowCount()
                {
                    return rowCount;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    throw new IllegalArgumentException("Vector has no columns");
                }
            };
        }

        List<ValueVector> vectors = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            vectors.add(builders.get(i)
                    .build());
        }
        return TupleVector.of(new Schema(columns), vectors);
    }

    private void appendNull(ABufferVectorBuilder builder, int count)
    {
        builder.ensureSize(count);
        builder.putNulls(true, count);
        builder.put(true, null, -1, count);
        builder.size += count;
    }

    private void appendInternal(TupleVector vector, int filterFrom, int filterTo, IntPredicate filter)
    {
        int start = 0;
        int end = 0;
        for (int i = filterFrom; i < filterTo; i++)
        {
            boolean predicateResult = filter.test(i);

            if (predicateResult)
            {
                end++;
            }

            // Append rows if we ended a true chain or if we got true at the last row
            if ((!predicateResult
                    && start != end)
                    || (predicateResult
                            && i == filterTo - 1))
            {
                append(vector, start, end);
            }

            if (!predicateResult)
            {
                // Reset counters
                end++;
                start = end;
            }
        }
    }

    private boolean columnsEquals(List<Column> existingColumns, List<Column> schemaColumns)
    {
        int size = existingColumns.size();
        if (size != schemaColumns.size())
        {
            return false;
        }

        for (int i = 0; i < size; i++)
        {
            if (!existingColumns.get(i)
                    .equals(schemaColumns.get(i)))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Verifies schema for a new inner tuple vector. Return an array with columns mappings for the new vector, that is ordinal in resulting inner columns to ordinal in provided vectors schema.
     */
    private Int2IntMap verifySchema(BufferAllocator allocator, int estimatedRowCount, TupleVector vector, List<Column> existingColumns, List<ABufferVectorBuilder> builders)
    {
        // We are still on the same schema, noting to do
        if (columnsEquals(existingColumns, vector.getSchema()
                .getColumns()))
        {
            return EMPTY;
        }

        int currentSize = existingColumns.size();
        int newSize = vector.getSchema()
                .getSize();

        if (currentSize == 0)
        {
            existingColumns.addAll(vector.getSchema()
                    .getColumns());
            for (int i = 0; i < newSize; i++)
            {
                builders.add(ABufferVectorBuilder.getBuilder(vector.getSchema()
                        .getColumns()
                        .get(i)
                        .getType(), allocator, estimatedRowCount));
            }
            return EMPTY;
        }

        List<Column> newColumns = new ArrayList<>(vector.getSchema()
                .getColumns());

        int currentRowCount = builders.get(0).size;

        Int2IntMap mapping = new Int2IntOpenHashMap(Math.max(currentSize, newSize));

        for (int i = 0; i < currentSize; i++)
        {
            int newIndex = -1;

            Column currentColumn = existingColumns.get(i);

            for (int j = 0; j < newSize; j++)
            {
                if (currentColumn.equals(newColumns.get(j)))
                {
                    newIndex = j;
                    break;
                }
            }

            // Existing column
            if (newIndex >= 0)
            {
                newColumns.set(newIndex, null);
                mapping.put(i, newIndex);
            }
            else
            {
                // Mark that current column doesn't exist in appending vector
                mapping.put(i, -1);
            }
        }

        // Add new columns last
        for (int i = 0; i < newSize; i++)
        {
            Column newColumn = newColumns.get(i);
            if (newColumn == null)
            {
                continue;
            }

            mapping.put(existingColumns.size(), i);
            existingColumns.add(newColumn);
            ABufferVectorBuilder builder = ABufferVectorBuilder.getBuilder(newColumn.getType(), allocator, estimatedRowCount);
            // Adjust new vector builder according to current row count in this builder
            appendNull(builder, currentRowCount);
            builders.add(builder);
        }

        return mapping;
    }
}
