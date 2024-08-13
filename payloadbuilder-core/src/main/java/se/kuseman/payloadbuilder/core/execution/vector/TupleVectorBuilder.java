package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntPredicate;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.ITupleVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Builder that builds a tuple vector by appending one to many tuple vectors and concatenating their value vectors
 */
class TupleVectorBuilder implements ITupleVectorBuilder
{
    private static final Int2IntMap EMPTY = new Int2IntOpenHashMap(0);

    private final VectorFactory factory;
    private final int estimatedRowCount;

    /** Resulting vectors */
    private List<MutableValueVector> vectors;
    /** Current inner columns */
    private List<Column> columns;
    /** Resulting row count to tuple vector */
    private int rowCount;

    /**
     * Optimization fields for cases where a single row is copied from a vector. Then we don't create mutable vectors etc. and copy columns etc. but instead create a new tuple vector with literal
     * values for that row.
     */
    private TupleVector singleRowTupleVector;

    TupleVectorBuilder(VectorFactory factory, int estimatedRowCount)
    {
        this.factory = requireNonNull(factory, "factory");
        this.estimatedRowCount = estimatedRowCount;
    }

    /** Append a populated result from provided outer and inner vector plus filter */
    @Override
    public void appendPopulate(TupleVector outer, TupleVector inner, ValueVector filter, String populateAlias)
    {
        final int outerSize = outer.getSchema()
                .getSize();
        int outerRowCount = outer.getRowCount();
        int innerRowCount = inner.getRowCount();

        final Schema schema = SchemaUtils.joinSchema(outer.getSchema(), inner.getSchema(), populateAlias);
        for (int i = 0; i < outerRowCount; i++)
        {
            int filterStart = i * innerRowCount;
            int filterEnd = filterStart + innerRowCount;

            // TODO: could cache bitset for matched inner rows and reuse created inner vector
            TupleVectorBuilder b = new TupleVectorBuilder(factory, innerRowCount);
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
    @Override
    public void append(TupleVector vector)
    {
        int rowCount = vector.getRowCount();
        if (rowCount == 0
                && vector.getSchema()
                        .getSize() == 0)
        {
            return;
        }

        // Postpone creating columns etc. if this is a single row build
        // then we can save alot
        if (vectors == null
                && singleRowTupleVector == null
                && rowCount == 1)
        {
            singleRowTupleVector = createSingleRowTupleVector(vector, 0);
            return;
        }

        append(vector, 0, rowCount, true);
    }

    /** Append a vector appending it's vectors to buffers using provided filter */
    @Override
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
    @Override
    public void append(TupleVector vector, BitSet filter)
    {
        appendInternal(vector, 0, vector.getRowCount(), row -> filter.get(row));
    }

    /** Append a new vector to builder, appending it's vectors to buffers */
    private void append(TupleVector vector, int from, int to, boolean checkSingleRowVector)
    {
        if (vectors == null)
        {
            // Append the previous single tuple vector first
            if (checkSingleRowVector
                    && singleRowTupleVector != null)
            {
                append(singleRowTupleVector, 0, 1, false);
                singleRowTupleVector = null;
            }
            else
            {
                vectors = new ArrayList<>(vector.getSchema()
                        .getSize());
                columns = new ArrayList<>(vector.getSchema()
                        .getSize());
            }
        }

        Int2IntMap mapping = verifySchema(vector);

        int length = to - from;
        rowCount += length;
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            MutableValueVector resultVector = vectors.get(i);
            int columnIndex = mapping.isEmpty() ? i
                    : mapping.get(i);
            // Append null
            if (columnIndex < 0)
            {
                resultVector.copy(resultVector.size(), ValueVector.literalNull(ResolvedType.of(Type.Any), length));
            }
            else
            {
                ValueVector v = vector.getColumn(columnIndex);
                resultVector.copy(resultVector.size(), v, from, length);
            }
        }
    }

    /** Build resulting tuple vector */
    @Override
    public TupleVector build()
    {
        if (vectors == null)
        {
            if (singleRowTupleVector != null)
            {
                return singleRowTupleVector;
            }

            return TupleVector.EMPTY;
        }

        int size = vectors.size();

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
        return TupleVector.of(new Schema(columns), vectors);
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
                append(vector, start, end, true);
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
            // NOTE! We only compare column names here and not it's type
            // When running a asterisk query we might end up with same column names but different types.
            // For example when having a left join with no matching inner rows we use the compile time schema
            // for the inner and those will be ANY and if combined with a vector with rows we will have the real runtime
            // type and we will end up with a wrong resulting vector.
            if (!StringUtils.equalsIgnoreCase(existingColumns.get(i)
                    .getName(),
                    schemaColumns.get(i)
                            .getName()))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Verifies schema for a new inner tuple vector. Return an array with columns mappings for the new vector, that is ordinal in resulting inner columns to ordinal in provided vectors schema.
     */
    private Int2IntMap verifySchema(TupleVector vector)
    {
        // We are still on the same schema, noting to do
        if (columnsEquals(columns, vector.getSchema()
                .getColumns()))
        {
            return EMPTY;
        }

        int currentSize = columns.size();
        int newSize = vector.getSchema()
                .getSize();

        List<Column> newColumns = new ArrayList<>(vector.getSchema()
                .getColumns());

        if (currentSize == 0)
        {
            columns.addAll(newColumns);
            for (int i = 0; i < newSize; i++)
            {
                vectors.add(factory.getMutableVector(columns.get(i)
                        .getType(), estimatedRowCount));
            }
            return EMPTY;
        }

        int currentRowCount = vectors.get(0)
                .size();

        Int2IntMap mapping = new Int2IntOpenHashMap(Math.max(currentSize, newSize));

        for (int i = 0; i < currentSize; i++)
        {
            int newIndex = -1;

            Column currentColumn = columns.get(i);

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

            mapping.put(columns.size(), i);
            columns.add(newColumn);
            MutableValueVector newVector = factory.getMutableVector(newColumn.getType(), estimatedRowCount);
            // Adjust new vector builder according to current row count in this builder
            newVector.copy(newVector.size(), ValueVector.literalNull(ResolvedType.of(Type.Any), currentRowCount));
            vectors.add(newVector);
        }

        return mapping;
    }

    /** Copipes one row from each vector into a resulting tuple vector. */
    private TupleVector createSingleRowTupleVector(TupleVector appendingVector, int row)
    {
        int size = appendingVector.getSchema()
                .getSize();
        List<ValueVector> vectors = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ValueVector vector = appendingVector.getColumn(i);
            Type type = vector.type()
                    .getType();
            ResolvedType schemaType = appendingVector.getSchema()
                    .getColumns()
                    .get(i)
                    .getType();
            if (vector.isNull(row))
            {
                vectors.add(ValueVector.literalNull(schemaType, 1));
                continue;
            }

            vectors.add(switch (type)
            {
                case Any -> ValueVector.literalAny(1, vector.getAny(row));
                case Array -> ValueVector.literalArray(vector.getArray(row), schemaType, 1);
                case Boolean -> ValueVector.literalBoolean(vector.getBoolean(row), 1);
                case DateTime -> vector.getDateTime(row);
                case DateTimeOffset -> vector.getDateTimeOffset(row);
                case Decimal -> vector.getDecimal(row);
                case String -> vector.getString(row);
                case Double -> ValueVector.literalDouble(vector.getDouble(row), 1);
                case Float -> ValueVector.literalFloat(vector.getFloat(row), 1);
                case Int -> ValueVector.literalInt(vector.getInt(row), 1);
                case Long -> ValueVector.literalLong(vector.getLong(row), 1);
                case Object -> ValueVector.literalObject(vector.getObject(row), schemaType, 1);
                case Table -> ValueVector.literalTable(vector.getTable(row), schemaType, 1);
            });
        }

        return TupleVector.of(appendingVector.getSchema(), vectors);
    }
}
