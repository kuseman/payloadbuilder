package se.kuseman.payloadbuilder.api.utils;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.EpochDateTime;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;

/** Utils for {@link ValueVector} and {@link TupleVector} */
public class VectorUtils
{
    private static final int START = 17;
    private static final int CONSTANT = 37;

    /**
     * Returns a tuple vector that is a cartesian product of provided input vectors. This structure reuses the data in the source vectors to avoid allocating to much.
     */
    public static TupleVector cartesian(final TupleVector outer, final TupleVector inner)
    {
        /*
         * @formatter:off
         * To achieve cartesian product we need to multiply and replicate the data in the input vectors
         * like this:
         * 
         * Outer
         *   a.id    [1,2,3]
         *   a.col   [4,5,6]
         *   
         * Inner
         *   v.a_id  [7, 8]
         *   v.col   [10,11]
         * 
         * We normalize the vectors be the inner row count to achieve
         * a cartesian product (all combinations).
         * 
         * The outer vector is replicated (row-wise)
         * The inner vector is replicated (vector-wise)
         * 
         * Outer
         * 0: 0 / 2 = 0
         * 1: 1 / 2 = 0
         * 2: 2 / 2 = 1
         * 3: 3 / 2 = 1
         * 4: 4 / 2 = 2
         * 5: 5 / 2 = 2
         * 
         * Inner:
         * 0: 0 % 2 = 0
         * 1: 1 % 2 = 1
         * 2: 2 % 2 = 0
         * 3: 3 % 2 = 1
         * 4: 4 % 2 = 0
         * 5: 5 % 2 = 1
         * 
         * Inner row count: 2
         * Outer
         *   a.id    [1,1][2,2][3,3]
         *   a.col   [4,4][5,5][6,6]
         * 
         * Inner
         *   v.a_id  [7,  8][7,  8][7,  8]
         *   v.col   [10,11][10,11][10,11]
         * 
         * Resulting tuplevector will be
         * 
         *  a.id    [1, 1, 2, 2, 3, 3]
         *  a.col   [4, 4, 5, 5, 6, 6]
         *  v.a_id  [7, 8, 7, 8, 7, 8]
         *  v.col   [10,11,10,11,10,11]
         *  @formatter:on
         */

        Schema outerSchema = outer.getSchema();
        final int outerColumnCount = outerSchema.getSize();
        final int outerRowCount = outer.getRowCount();
        final int innerRowCount = inner.getRowCount();
        final int rowCount = outerRowCount * innerRowCount;
        final Schema schema = Schema.concat(outer.getSchema(), inner.getSchema());

        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(final int column)
            {
                final boolean outerAccess = column < outerColumnCount;
                ValueVector vector = outerAccess ? outer.getColumn(column)
                        : inner.getColumn(column - outerColumnCount);

                return new ValueVectorAdapter(vector)
                {
                    @Override
                    public int size()
                    {
                        // All vectors has the cartesian size
                        return rowCount;
                    }

                    @Override
                    protected int getRow(int row)
                    {
                        if (outerAccess)
                        {
                            return row / innerRowCount;
                        }

                        return row % innerRowCount;
                    }
                };
            }
        };
    }

    /**
     * Concats two vectors into one. Takes all columns from the first one and appends to the second one. Requires that both vectors has the same row counts
     */
    public static TupleVector concat(final TupleVector vector1, final TupleVector vector2)
    {
        if (vector2 == null)
        {
            return vector1;
        }
        else if (vector1 == null)
        {
            return vector2;
        }

        if (vector1.getRowCount() != vector2.getRowCount())
        {
            throw new IllegalArgumentException("Vectors must have equal row counts");
        }

        final int size1 = vector1.getSchema()
                .getSize();
        List<Column> columns = new ArrayList<>(vector1.getSchema()
                .getColumns());
        columns.addAll(vector2.getSchema()
                .getColumns());
        final Schema schema = new Schema(columns);
        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return vector1.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                if (column < size1)
                {
                    return vector1.getColumn(column);
                }
                return vector2.getColumn(column - size1);
            }
        };
    }

    /** Concats two vectors to acts as one */
    public static ValueVector concat(final ValueVector vector1, final ValueVector vector2)
    {
        if (!Objects.equals(vector1.type(), vector2.type()))
        {
            throw new IllegalArgumentException("Concating two vectors requires the same data type. " + vector1.type() + " <> " + vector2.type());
        }

        final boolean nullable = vector1.isNullable()
                || vector2.isNullable();
        final int firstSize = vector1.size();
        final int totalSize = firstSize + vector2.size();
        return new ValueVectorAdapter(row ->
        {
            // Return the first or second value vector
            if (row < firstSize)
            {
                return vector1;
            }
            return vector2;

        }, totalSize, nullable, vector1.type())
        {
            @Override
            protected int getRow(int row)
            {
                // Adapt the row depending on if we target the first of second vector
                if (row < firstSize)
                {
                    return row;
                }
                return row - firstSize;
            }
        };
    }

    /** Merges a list of vectors into a single vector */
    public static TupleVector merge(final List<TupleVector> vectors)
    {
        /*
         * @formatter:off
         * Old
         * col1  [1,2,3]
         * col2  [4,5,6]
         * 
         * New
         * 
         * col2  [6,7]
         * col3  [true,false]
         * 
         * Merge:
         * 
         * col1  [1,2,3]                  
         * col2  [4,5,6] [6,7]            
         * col3          [true,false]     Null on index 0,1,2.
         * 
         * If a row wanted is larger than current columns size then return null
         * @formatter:on
         */
        final int vectorCount = vectors.size();
        if (vectorCount == 1)
        {
            return vectors.get(0);
        }

        final TupleVector first = vectors.get(0);
        List<Column> columns = new ArrayList<>(first.getSchema()
                .getColumns());
        final List<ValueVector> valueVectors = new ArrayList<>();

        // Copy the first vectors
        for (int i = 0; i < columns.size(); i++)
        {
            valueVectors.add(first.getColumn(i));
        }

        // TODO: this can be optimized by comparing the whole schema, if it's equal then
        // we can simple concat the value vectors

        int tmp = first.getRowCount();
        int columnCount = columns.size();
        for (int i = 1; i < vectorCount; i++)
        {
            TupleVector vector = vectors.get(i);
            Schema schema = vector.getSchema();

            int currentRowCount = vector.getRowCount();

            // We have the same schema as previous vector, merge value vectors
            if (columns.equals(schema.getColumns()))
            {
                for (int j = 0; j < columnCount; j++)
                {
                    ValueVector existingVector = valueVectors.get(j);

                    // Do we need to pad nulls to existing vector to be on par with total row count?
                    if (existingVector.size() < tmp)
                    {
                        existingVector = concat(existingVector, ValueVector.literalNull(existingVector.type(), tmp - existingVector.size()));
                    }

                    valueVectors.set(j, concat(existingVector, vector.getColumn(j)));
                }
            }
            else
            {
                // If match then append to existing column also increase row count
                // If no match then add a new column to schema and a new value vector
                // padded with nulls for existing row count and increase row count
                // only if current vectors row count is larger than previous
                int currentColumnCount = schema.getColumns()
                        .size();
                for (int c = 0; c < currentColumnCount; c++)
                {
                    Column column = schema.getColumns()
                            .get(c);
                    boolean match = false;

                    for (int j = 0; j < columnCount; j++)
                    {
                        // Found a match
                        if (column.equals(columns.get(j)))
                        {
                            match = true;
                            ValueVector existingVector = valueVectors.get(j);
                            ValueVector vectorToAdd = vector.getColumn(c);
                            valueVectors.set(j, concat(existingVector, vectorToAdd));
                            break;
                        }
                    }

                    // New column, append last
                    if (!match)
                    {
                        columns.add(column);
                        columnCount++;

                        ValueVector newVector = vector.getColumn(c);
                        // A new column needs to be appended last and nulls padded with current row size before
                        // Create a null pad vector once with the previous row count
                        ValueVector nulls = ValueVector.literalNull(newVector.type(), tmp);
                        valueVectors.add(concat(nulls, newVector));
                    }
                }
            }

            tmp += currentRowCount;
        }

        final Schema schema = new Schema(columns);
        // All vectors equals in size so pick the first one as row count
        final int rowCount = tmp;
        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return new ValueVectorAdapter(valueVectors.get(column))
                {
                    @Override
                    public int size()
                    {
                        return rowCount;
                    }

                    @Override
                    public boolean isNullable()
                    {
                        // We have nulls since the current value vector is smaller than the total row count
                        if (rowCount > wrapped.size())
                        {
                            return true;
                        }
                        return wrapped.isNullable();
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        // Adjust for a smaller vector than total row count
                        if (row >= wrapped.size())
                        {
                            return true;
                        }
                        return wrapped.isNull(row);
                    }

                    @Override
                    protected int getRow(int row)
                    {
                        if (row >= wrapped.size())
                        {
                            throw new IllegalArgumentException("Illegal row number");
                        }
                        return row;
                    }
                };
            }
        };
    }

    /** Hash a row from an array of value vectors */
    public static int hash(ValueVector[] vectors, int row)
    {
        int hash = START;
        int size = vectors.length;
        for (int i = 0; i < size; i++)
        {
            ValueVector vv = vectors[i];

            if (vv.isNullable()
                    && vv.isNull(row))
            {
                hash = hash * CONSTANT;
                continue;
            }

            switch (vv.type()
                    .getType())
            {
                // Code from commons HashCodeBuilder
                case Boolean:
                    hash = hash * CONSTANT + (vv.getBoolean(row) ? 0
                            : 1);
                    break;
                case Double:
                    double doubleValue = vv.getDouble(row);
                    long l = Double.doubleToLongBits(doubleValue);
                    hash = hash * CONSTANT + ((int) (l ^ (l >> 32)));
                    break;
                case Float:
                    hash = hash * CONSTANT + Float.floatToIntBits(vv.getFloat(row));
                    break;
                case Int:
                    hash = hash * CONSTANT + vv.getInt(row);
                    break;
                case Long:
                    long longValue = vv.getLong(row);
                    hash = hash * CONSTANT + ((int) (longValue ^ (longValue >> 32)));
                    break;
                case String:
                    hash = hash * CONSTANT + vv.getString(row)
                            .hashCode();
                    break;
                case DateTime:
                    hash = hash * CONSTANT + (int) vv.getDateTime(row)
                            .getEpoch();
                    break;
                case Any:
                    hash = hash * CONSTANT + Objects.hashCode(vv.getValue(row));
                    break;
                default:
                    throw new IllegalArgumentException("Hashing of type " + vv.type()
                            .toTypeString() + " is not supported");
            }
        }
        return hash;
    }

    /**
     * Perform equals on an array of value vectors. Compares row1 against row2
     */
    public static boolean equals(ValueVector[] vectors, int row1, int row2)
    {
        int size = vectors.length;
        for (int i = 0; i < size; i++)
        {
            ValueVector a = vectors[i];
            ValueVector b = vectors[i];

            // CSOFF
            boolean aNull = a.isNullable()
                    && a.isNull(row1);
            boolean bNull = b.isNullable()
                    && b.isNull(row2);
            // CSON
            if (aNull != bNull)
            {
                return false;
            }
            else if (aNull)
            {
                continue;
            }

            switch (a.type()
                    .getType())
            {
                case Boolean:
                    if (a.getBoolean(row1) != b.getBoolean(row2))
                    {
                        return false;
                    }
                    break;
                case Double:
                    if (a.getDouble(row1) != b.getDouble(row2))
                    {
                        return false;
                    }
                    break;
                case Float:
                    if (a.getFloat(row1) != b.getFloat(row2))
                    {
                        return false;
                    }
                    break;
                case Int:
                    if (a.getInt(row1) != b.getInt(row2))
                    {
                        return false;
                    }
                    break;
                case Long:
                    if (a.getLong(row1) != b.getLong(row2))
                    {
                        return false;
                    }
                    break;
                case String:
                    UTF8String aref = a.getString(row1);
                    UTF8String bref = b.getString(row2);
                    if (!aref.equals(bref))
                    {
                        return false;
                    }
                    break;
                case DateTime:
                    EpochDateTime adate = a.getDateTime(row1);
                    EpochDateTime bdate = b.getDateTime(row2);
                    if (!adate.equals(bdate))
                    {
                        return false;
                    }
                    break;
                case Any:
                    if (!ExpressionMath.eq(a.getValue(row1), b.getValue(row2)))
                    {
                        return false;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Performing equal of type " + a.type()
                            .toTypeString() + " is not supported");
            }
        }
        return true;
    }

    /** Converts provided value to a {@link ValueVector}. Transforms List/Collections/Arrays to value vector */
    public static ValueVector convertToValueVector(final Object value)
    {
        requireNonNull(value);

        if (value instanceof List)
        {
            @SuppressWarnings("unchecked")
            final List<Object> list = (List<Object>) value;
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.of(Type.Any);
                }

                @Override
                public int size()
                {
                    return list.size();
                }

                @Override
                public boolean isNull(int row)
                {
                    return list.get(row) == null;
                }

                @Override
                public Object getValue(int row)
                {
                    return list.get(row);
                }
            };
        }
        else if (value instanceof Collection)
        {
            // Turn collection into a list
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) value;
            return convertToValueVector(new ArrayList<>(collection));
        }
        else if (value != null
                && value.getClass()
                        .isArray())
        {
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.of(Type.Any);
                }

                @Override
                public int size()
                {
                    return Array.getLength(value);
                }

                @Override
                public boolean isNull(int row)
                {
                    return Array.get(value, row) == null;
                }

                @Override
                public Object getValue(int row)
                {
                    return Array.get(value, row);
                }
            };
        }

        // Wrap object as a single item value vector
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public int size()
            {
                return 1;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public boolean isNullable()
            {
                return false;
            }

            @Override
            public Object getValue(int row)
            {
                return value;
            }
        };
    }
}
