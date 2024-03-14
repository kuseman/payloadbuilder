package se.kuseman.payloadbuilder.core.execution;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Utils for {@link ValueVector} and {@link TupleVector} */
public class VectorUtils
{
    private static final int START = 17;
    private static final int CONSTANT = 37;

    /**
     * Tries to determine a known type of this vector if the vectors type is ANY. Picks the first non null value. NOTE! There could be mixed types in vector so it's just an estimation
     */
    public static Column.Type getAnyType(ValueVector v)
    {
        int size = v.size();
        if (size == 0)
        {
            return Type.Any;
        }

        for (int i = 0; i < size; i++)
        {
            if (v.isNull(i))
            {
                continue;
            }

            Object val = v.getAny(i);
            if (val instanceof String
                    || val instanceof UTF8String)
            {
                return Column.Type.String;
            }
            else if (val instanceof Integer)
            {
                return Column.Type.Int;
            }
            else if (val instanceof Long)
            {
                return Column.Type.Long;
            }
            else if (val instanceof Float)
            {
                return Column.Type.Float;
            }
            else if (val instanceof Double)
            {
                return Column.Type.Double;
            }
            else if (val instanceof Boolean)
            {
                return Column.Type.Boolean;
            }
            else if (val instanceof Decimal
                    || val instanceof BigDecimal)
            {
                return Column.Type.Decimal;
            }
            else if (val instanceof EpochDateTime
                    || val instanceof LocalDateTime)
            {
                return Column.Type.DateTime;
            }
            else if (val instanceof EpochDateTimeOffset
                    || val instanceof ZonedDateTime)
            {
                return Column.Type.DateTimeOffset;
            }
        }

        return Column.Type.Any;
    }

    /** Transforms a cartesian vector into a populated variant based on provided outer tuple vector */
    public static TupleVector populateCartesian(final TupleVector outer, final TupleVector inner, String populateAlias)
    {
        requireNonNull(populateAlias, "populateAlias");
        final Schema schema = SchemaUtils.joinSchema(outer.getSchema(), inner.getSchema(), populateAlias);
        final int outerSize = outer.getSchema()
                .getSize();
        final int rowCount = outer.getRowCount();
        final ValueVector innerValueVector = ValueVector.literalTable(inner, rowCount);

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
                if (column < outerSize)
                {
                    return outer.getColumn(column);
                }
                return innerValueVector;
            }
        };
    }

    /**
     * Produces a cartesian product between two vectors. Vector1's rows are replicated Vector2's vectors are replicated
     */
    public static TupleVector cartesian(final TupleVector outer, final TupleVector inner)
    {
        final Schema schema = SchemaUtils.joinSchema(outer.getSchema(), inner.getSchema());
        final int outerSize = outer.getSchema()
                .getSize();
        final int innerRowcount = inner.getRowCount();
        final int rowCount = outer.getRowCount() * innerRowcount;

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
                final boolean isOuter = column < outerSize;
                ValueVector value = isOuter ? outer.getColumn(column)
                        : inner.getColumn(column - outerSize);

                return new ValueVectorAdapter(value)
                {
                    @Override
                    public int size()
                    {
                        return rowCount;
                    }

                    @Override
                    protected int getRow(int row)
                    {
                        if (isOuter)
                        {
                            return row / innerRowcount;
                        }
                        return row % innerRowcount;
                    };
                };
            }
        };
    }

    /** Hash a row from an array of value vectors */
    public static int hash(ValueVector[] vectors, int row)
    {
        return hash(vectors, null, row);
    }

    /** Hash a row from an array of value vectors */
    public static int hash(ValueVector[] vectors, Column.Type[] types, int row)
    {
        int hash = START;
        int size = vectors.length;
        for (int i = 0; i < size; i++)
        {
            ValueVector vv = vectors[i];

            if (vv.isNull(row))
            {
                hash = hash * CONSTANT;
                continue;
            }

            Column.Type type = types == null ? vv.type()
                    .getType()
                    : types[i];

            // CSOFF
            switch (type)
            // CSON
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
                case Decimal:
                    hash = hash * CONSTANT + vv.getDecimal(row)
                            .hashCode();
                    break;
                case String:
                    hash = hash * CONSTANT + vv.getString(row)
                            .hashCode();
                    break;
                case DateTime:
                    hash = hash * CONSTANT + (int) vv.getDateTime(row)
                            .getEpoch();
                    break;
                case DateTimeOffset:
                    hash = hash * CONSTANT + (int) vv.getDateTimeOffset(row)
                            .getEpoch();
                    break;
                case Any:
                    hash = hash * CONSTANT + Objects.hashCode(vv.getAny(row));
                    break;
                case Object:
                case Array:
                case Table:
                    throw new IllegalArgumentException("Hashing of type " + vv.type()
                            .toTypeString() + " is not supported");
                // No default case here!!!
            }
        }
        return hash;
    }

    /**
     * Perform equals on an array of value vectors. Compares row1 against row2
     */
    public static boolean equals(ValueVector[] vectors1, ValueVector[] vectors2, int row1, int row2)
    {
        int size = vectors1.length;
        if (size != vectors2.length)
        {
            return false;
        }

        for (int i = 0; i < size; i++)
        {
            ValueVector a = vectors1[i];
            ValueVector b = vectors2[i];

            Type typeA = a.type()
                    .getType();
            Type typeB = b.type()
                    .getType();

            Type resultType = typeA.getPrecedence() > typeB.getPrecedence() ? typeA
                    : typeB;

            if (!equals(a, b, resultType, row1, row2))
            {
                return false;
            }
        }
        return true;
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

            Type typeA = a.type()
                    .getType();
            Type typeB = b.type()
                    .getType();

            Type resultType = typeA.getPrecedence() > typeB.getPrecedence() ? typeA
                    : typeB;

            if (!equals(a, b, resultType, row1, row2))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Perform equals on an array of value vectors. Compares row1 against row2
     */
    public static boolean equals(ValueVector vector1, ValueVector vector2, Type type, int row1, int row2)
    {
        return equals(vector1, vector2, type, row1, row2, true);
    }

    /**
     * Perform equals on an array of value vectors. Compares row1 against row2
     */
    public static boolean equals(ValueVector vector1, ValueVector vector2, Type type, int row1, int row2, boolean compareNull)
    {
        if (compareNull)
        {
            // CSOFF
            boolean aNull = vector1.isNull(row1);
            boolean bNull = vector2.isNull(row2);
            // CSON
            if (aNull != bNull)
            {
                return false;
            }
            else if (aNull)
            {
                return true;
            }
        }

        // CSOFF
        switch (type)
        // CSON
        {
            case Boolean:
                if (vector1.getBoolean(row1) != vector2.getBoolean(row2))
                {
                    return false;
                }
                break;
            case Double:
                if (vector1.getDouble(row1) != vector2.getDouble(row2))
                {
                    return false;
                }
                break;
            case Float:
                if (vector1.getFloat(row1) != vector2.getFloat(row2))
                {
                    return false;
                }
                break;
            case Int:
                if (vector1.getInt(row1) != vector2.getInt(row2))
                {
                    return false;
                }
                break;
            case Long:
                if (vector1.getLong(row1) != vector2.getLong(row2))
                {
                    return false;
                }
                break;
            case Decimal:
                // NOTE! Use compare instead of equals here else we will get weird results
                if (vector1.getDecimal(row1)
                        .compareTo(vector2.getDecimal(row2)) != 0)
                {
                    return false;
                }
                break;
            case String:
                UTF8String astr = vector1.getString(row1);
                UTF8String bstr = vector2.getString(row2);
                if (!astr.equals(bstr))
                {
                    return false;
                }
                break;
            case DateTime:
                EpochDateTime adate = vector1.getDateTime(row1);
                EpochDateTime bdate = vector2.getDateTime(row2);
                if (!adate.equals(bdate))
                {
                    return false;
                }
                break;
            case DateTimeOffset:
                EpochDateTimeOffset adateO = vector1.getDateTimeOffset(row1);
                EpochDateTimeOffset bdateO = vector2.getDateTimeOffset(row2);
                if (!adateO.equals(bdateO))
                {
                    return false;
                }
                break;
            case Any:
                if (ExpressionMath.cmp(vector1.getAny(row1), vector2.getAny(row2)) != 0)
                {
                    return false;
                }
                break;
            case Object:
            case Array:
            case Table:
                throw new IllegalArgumentException("Performing equal of type " + vector1.type()
                        .toTypeString() + " is not supported");
            // No default case here!!!
        }
        return true;
    }

    /**
     * Compare values to two rows. NOTE! Doesn't take nulls into consideration
     *
     * @param left Left vector
     * @param right Right vector
     * @param type The type to use when comparing values. This is not necessarily the same type as either left/right, it can be a promoted type.
     * @param leftRow Row index to use for left vector
     * @param rightRow Row index to use for right vector
     */
    public static int compare(ValueVector left, ValueVector right, Type type, int leftRow, int rightRow)
    {
        // CSOFF
        switch (type)
        // CSON
        {
            case Boolean:
                return Boolean.compare(left.getBoolean(leftRow), right.getBoolean(rightRow));
            case Double:
                return Double.compare(left.getDouble(leftRow), right.getDouble(rightRow));
            case Float:
                return Float.compare(left.getFloat(leftRow), right.getFloat(rightRow));
            case Int:
                return Integer.compare(left.getInt(leftRow), right.getInt(rightRow));
            case Long:
                return Long.compare(left.getLong(leftRow), right.getLong(rightRow));
            case Decimal:
                return left.getDecimal(leftRow)
                        .compareTo(right.getDecimal(rightRow));
            case String:
                UTF8String refL = left.getString(leftRow);
                UTF8String refR = right.getString(rightRow);
                return refL.compareTo(refR);
            case DateTime:
                EpochDateTime dateL = left.getDateTime(leftRow);
                EpochDateTime dateR = right.getDateTime(rightRow);
                return dateL.compareTo(dateR);
            case DateTimeOffset:
                EpochDateTimeOffset dateOL = left.getDateTimeOffset(leftRow);
                EpochDateTimeOffset dateOR = right.getDateTimeOffset(rightRow);
                return dateOL.compareTo(dateOR);
            case Any:
                // Reflective compare
                return ExpressionMath.cmp(left.getAny(leftRow), right.getAny(rightRow));
            case Array:
            case Table:
            case Object:
                throw new IllegalArgumentException("Cannot compare type " + type);
            // NO default case here!!!
        }
        throw new IllegalArgumentException("Cannot compare type " + type);
    }

    /**
     * Tries to convert provided object into a known Payloadbuilder type. Map -&gt; Object List/Array/Collection -&gt; Array else returns object as is.
     */
    public static Object convert(Object value)
    {
        Object result = convertToValueVector(value, false);
        if (result instanceof ValueVector)
        {
            return result;
        }
        return convertToObjectVector(result);
    }

    /** Converts provided value to a {@link ValueVector}. Transforms List/Collections/Arrays to value vector */
    public static ValueVector convertToValueVector(Object value)
    {
        return (ValueVector) convertToValueVector(value, true);
    }

    /** Converts provided value to a {@link ValueVector}. Transforms List/Collections/Arrays to value vector */
    public static Object convertToValueVector(final Object value, boolean wrapSingleValue)
    {
        requireNonNull(value);
        if (value instanceof ValueVector)
        {
            return value;
        }
        else if (value instanceof List)
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
                public Object getAny(int row)
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
                public Object getAny(int row)
                {
                    return Array.get(value, row);
                }
            };
        }

        if (!wrapSingleValue)
        {
            return value;
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
            public Object getAny(int row)
            {
                return value;
            }
        };
    }

    /**
     * Converts provided object to a {@link ObjectVector} is possible else returns object as is
     */
    public static Object convertToObjectVector(Object value)
    {
        if (value instanceof Map)
        {
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>) value;
            final Schema schema = new Schema(map.keySet()
                    .stream()
                    .map(k -> Column.of(k, ResolvedType.of(Type.Any)))
                    .collect(toList()));

            return new ObjectVector()
            {
                @Override
                public ValueVector getValue(int ordinal)
                {
                    Column column = schema.getColumns()
                            .get(ordinal);
                    Object value = map.get(column.getName());
                    return value == null ? ValueVector.literalNull(ResolvedType.of(Type.Any), 1)
                            : ValueVector.literalAny(1, value);
                }

                @Override
                public Schema getSchema()
                {
                    return schema;
                }
            };
        }

        return value;
    }

}
