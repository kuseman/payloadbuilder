package se.kuseman.payloadbuilder.api.execution;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** Definition of a value vector. All values for one column in a {@link TupleVector} batch. */
public interface ValueVector
{
    /** Return type of vector values */
    ResolvedType type();

    /** Return size of vector */
    int size();

    /** Return if value at provided row is null */
    boolean isNull(int row);

    /** Get string at provided row. */
    default UTF8String getString(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Boolean)
        {
            return UTF8String.from(getBoolean(row));
        }
        else if (type == Type.DateTime)
        {
            return UTF8String.from(getDateTime(row));
        }
        else if (type == Type.DateTimeOffset)
        {
            return UTF8String.from(getDateTimeOffset(row));
        }
        else if (type == Type.Decimal)
        {
            return UTF8String.from(getDecimal(row));
        }
        else if (type == Type.Double)
        {
            return UTF8String.from(getDouble(row));
        }
        else if (type == Type.Float)
        {
            return UTF8String.from(getFloat(row));
        }
        else if (type == Type.Int)
        {
            return UTF8String.from(getInt(row));
        }
        else if (type == Type.Long)
        {
            return UTF8String.from(getInt(row));
        }
        return UTF8String.from(getAny(row));
    }

    /** Get date time at provided row */
    default EpochDateTime getDateTime(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Long)
        {
            return EpochDateTime.from(getLong(row));
        }
        else if (type == Type.String)
        {
            return EpochDateTime.from(getString(row).toString());
        }
        else if (type == Type.DateTimeOffset)
        {
            return EpochDateTime.from(getDateTimeOffset(row));
        }

        return EpochDateTime.from(getAny(row));
    }

    /** Get date time offset at provided row */
    default EpochDateTimeOffset getDateTimeOffset(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Long)
        {
            return EpochDateTimeOffset.from(getLong(row));
        }
        else if (type == Type.String)
        {
            return EpochDateTimeOffset.from(getString(row).toString());
        }
        else if (type == Type.DateTime)
        {
            return EpochDateTimeOffset.from(getDateTime(row));
        }

        return EpochDateTimeOffset.from(getAny(row));
    }

    /** Get boolean value for provided row */
    default boolean getBoolean(int row)
    {
        ResolvedType resolvedType = type();
        Type type = resolvedType.getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row) != 0;
        }
        else if (type == Type.Long)
        {
            return getLong(row) != 0;
        }
        Object value = getAny(row);
        return toBoolean(resolvedType, value);
    }

    /** Get int value for provided row */
    default int getInt(int row)
    {
        ResolvedType resolvedType = type();
        Type type = resolvedType.getType();

        // Implicit casts
        if (type == Type.Long)
        {
            return (int) getLong(row);
        }
        else if (type == Type.Decimal)
        {
            return getDecimal(row).intValue();
        }
        else if (type == Type.Float)
        {
            return (int) getFloat(row);
        }
        else if (type == Type.Double)
        {
            return (int) getDouble(row);
        }
        else if (type == Type.Boolean)
        {
            return getBoolean(row) ? 1
                    : 0;
        }
        Object value = getAny(row);
        return toInt(resolvedType, value);
    }

    /** Get long value for provided row */
    default long getLong(int row)
    {
        ResolvedType resolvedType = type();
        Type type = resolvedType.getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
        }
        else if (type == Type.Decimal)
        {
            return getDecimal(row).longValue();
        }
        else if (type == Type.Float)
        {
            return (long) getFloat(row);
        }
        else if (type == Type.Double)
        {
            return (long) getDouble(row);
        }
        else if (type == Type.Boolean)
        {
            return getBoolean(row) ? 1L
                    : 0L;
        }
        Object value = getAny(row);
        return toLong(resolvedType, value);
    }

    /** Get decimal value for provided row */
    default Decimal getDecimal(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return Decimal.from(getInt(row));
        }
        else if (type == Type.Long)
        {
            return Decimal.from(getLong(row));
        }
        else if (type == Type.Float)
        {
            return Decimal.from(getFloat(row));
        }
        else if (type == Type.Double)
        {
            return Decimal.from(getDouble(row));
        }
        else if (type == Type.Boolean)
        {
            return Decimal.from(getBoolean(row) ? 1
                    : 0);
        }
        return Decimal.from(getAny(row));
    }

    /** Get float value for provided row */
    default float getFloat(int row)
    {
        ResolvedType resolvedType = type();
        Type type = resolvedType.getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
        }
        else if (type == Type.Long)
        {
            return getLong(row);
        }
        else if (type == Type.Decimal)
        {
            return getDecimal(row).floatValue();
        }
        else if (type == Type.Double)
        {
            return (float) getDouble(row);
        }
        else if (type == Type.Boolean)
        {
            return getBoolean(row) ? 1F
                    : 0F;
        }
        Object value = getAny(row);
        return toFloat(resolvedType, value);
    }

    /** Get double value for provided row */
    default double getDouble(int row)
    {
        ResolvedType resolvedType = type();
        Type type = resolvedType.getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
        }
        else if (type == Type.Long)
        {
            return getLong(row);
        }
        else if (type == Type.Decimal)
        {
            return getDecimal(row).doubleValue();
        }
        else if (type == Type.Float)
        {
            return getFloat(row);
        }
        else if (type == Type.Boolean)
        {
            return getBoolean(row) ? 1D
                    : 0D;
        }
        Object value = getAny(row);
        return toDouble(resolvedType, value);
    }

    /**
     * Return value of provided row. NOTE! This method must not be used when checking if value is null. Use {@link ValueVector#isNull(int)}.
     */
    default Object getAny(int row)
    {
        Type type = type().getType();
        // CSOFF
        switch (type)
        // CSON
        {
            case Any:
                throw new IllegalArgumentException("Check implementation of ValueVector: " + getClass() + " for getAny");
            case Array:
                return getArray(row);
            case Boolean:
                return getBoolean(row);
            case DateTime:
                return getDateTime(row);
            case DateTimeOffset:
                return getDateTimeOffset(row);
            case Decimal:
                return getDecimal(row);
            case Double:
                return getDouble(row);
            case Float:
                return getFloat(row);
            case Int:
                return getInt(row);
            case Long:
                return getLong(row);
            case Object:
                return getObject(row);
            case String:
                return getString(row);
            case Table:
                return getTable(row);
            // NO default case here!!!
        }
        throw new IllegalArgumentException("Unsupported type: " + type + " for getValue");
    }

    /** Return object for provided row */
    default ObjectVector getObject(int row)
    {
        Type type = type().getType();
        if (type == Type.Any)
        {
            Object value = getAny(row);
            if (value instanceof ObjectVector)
            {
                return (ObjectVector) value;
            }

            throw new IllegalArgumentException("Cannot cast " + value + " to " + Type.Object);
        }
        else if (type == Type.Object)
        {
            throw new IllegalArgumentException("getObject not implemented on " + getClass());
        }
        throw new IllegalArgumentException("Cannot cast " + type + " to " + Type.Object);
    }

    /** Return array for provided row */
    default ValueVector getArray(int row)
    {
        Type type = type().getType();
        if (type == Type.Any)
        {
            Object value = getAny(row);
            if (value instanceof ValueVector)
            {
                return (ValueVector) value;
            }

            throw new IllegalArgumentException("Cannot cast " + value + " to " + Type.Array);
        }
        else if (type == Type.Array)
        {
            throw new IllegalArgumentException("getArray not implemented on " + getClass());
        }

        throw new IllegalArgumentException("Cannot cast " + type + " to " + Type.Array);
    }

    /** Return table for provided row */
    default TupleVector getTable(int row)
    {
        Type type = type().getType();
        if (type == Type.Any)
        {
            Object value = getAny(row);
            if (value instanceof TupleVector)
            {
                return (TupleVector) value;
            }

            throw new IllegalArgumentException("Cannot cast " + value + " to " + Type.Table);
        }
        else if (type == Type.Array)
        {
            // We can convert an empty array to an empty table
            ValueVector array = getArray(row);
            if (array.size() == 0)
            {
                return TupleVector.EMPTY;
            }
        }
        else if (type == Type.Object)
        {
            // We can convert an empty array to an empty table
            ObjectVector object = getObject(row);
            if (object.getSchema()
                    .getSize() == 0)
            {
                return TupleVector.EMPTY;
            }
        }
        else if (type == Type.Table)
        {
            throw new IllegalArgumentException("getTable not implemented on " + getClass());
        }
        throw new IllegalArgumentException("Cannot cast " + type + " to " + Type.Table);
    }

    /**
     * Return value as object include null value. Note! This method differs from {@link #getAny(int)} in that sense that this method converts the actual value to a boxed type and {@link #getAny(int)}
     * is the real accessor for the vectors actual type.
     */
    default Object valueAsObject(int row)
    {
        if (isNull(row))
        {
            return null;
        }

        switch (type().getType())
        {
            case Boolean:
                return getBoolean(row);
            case Double:
                return getDouble(row);
            case Float:
                return getFloat(row);
            case Int:
                return getInt(row);
            case Long:
                return getLong(row);
            case Decimal:
                return getDecimal(row);
            case String:
                return getString(row);
            case DateTime:
                return getDateTime(row);
            case DateTimeOffset:
                return getDateTimeOffset(row);
            case Array:
                return getArray(row);
            case Table:
                return getTable(row);
            case Object:
                return getObject(row);
            default:
                return getAny(row);
        }
    }

    /** Return value as Java string for provided row */
    default String valueAsString(int row)
    {
        Object value = valueAsObject(row);
        if (value == null)
        {
            return null;
        }
        return String.valueOf(value);
    }

    /** Get predicate boolean, ie. null is false */
    default boolean getPredicateBoolean(int row)
    {
        // A predicate result is true if non null and true
        return !isNull(row)
                && getBoolean(row);
    }

    /** Return the cardinality of this vector. Only applicable for boolean vectors. Used for predicates */
    default int getCardinality()
    {
        Type type = type().getType();
        if (!(type == Type.Boolean
                || type == Type.Any))
        {
            throw new IllegalArgumentException("Cardinality is only supported for boolean value vectors");
        }

        int count = 0;
        int size = size();
        for (int i = 0; i < size; i++)
        {
            if (getPredicateBoolean(i))
            {
                count++;
            }
        }
        return count;
    }

    /** Create a literal vector of type {@link Column.Type#Object} with provided value and size */
    static ValueVector literalObject(ObjectVector value, int size)
    {
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(ResolvedType.object(value.getSchema()), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public ObjectVector getObject(int row)
            {
                return value;
            }
        };
    }

    /**
     * Create a literal vector of type {@link Column.Type#Object} with provided value, type and size NOTE! This can create an object with a different type as the vector. This is used when having
     * asterisk schemas and one is the planned type and the other is the runtime type.
     */
    static ValueVector literalObject(ObjectVector value, ResolvedType type, int size)
    {
        if (type.getType() != Type.Object)
        {
            throw new IllegalArgumentException("Expected a Object type but got: " + type);
        }
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(type, size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public ObjectVector getObject(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Array} with provided value and size */
    static ValueVector literalArray(ValueVector value, int size)
    {
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(ResolvedType.array(value.type()), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public ValueVector getArray(int row)
            {
                return value;
            }
        };
    }

    /**
     * Create a literal vector of type {@link Column.Type#Array} with provided value, type and size NOTE! This can create an array with a different type as the vector. This is used when having
     * asterisk schemas and one is the planned type and the other is the runtime type.
     */
    static ValueVector literalArray(ValueVector value, ResolvedType type, int size)
    {
        if (type.getType() != Type.Array)
        {
            throw new IllegalArgumentException("Expected a Array type but got: " + type);
        }
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(type, size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public ValueVector getArray(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Table} with provided value and size */
    static ValueVector literalTable(TupleVector value, int size)
    {
        return literalTable(value, ResolvedType.table(value.getSchema()), size);
    }

    /**
     * Create a literal vector of type {@link Column.Type#Table} with provided value, type and size NOTE! This can create a table with a different type as the vector. This is used when having asterisk
     * schemas and one is the planned type and the other is the runtime type.
     */
    static ValueVector literalTable(final TupleVector value, ResolvedType type, int size)
    {
        if (type.getType() != Type.Table)
        {
            throw new IllegalArgumentException("Expected a Table type but got: " + type);
        }
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(type, size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public TupleVector getTable(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Table} with provided values */
    static ValueVector literalTable(TupleVector... values)
    {
        return new LiteralValueVector(ResolvedType.table(values[0].getSchema()), values.length)
        {
            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public TupleVector getTable(int row)
            {
                return values[row];
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#DateTime} with provided value and size */
    static ValueVector literalDateTime(EpochDateTime value, int size)
    {
        requireNonNull(value);
        if (size == 1)
        {
            return value;
        }

        return new LiteralValueVector(ResolvedType.of(Type.DateTime), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public EpochDateTime getDateTime(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#DateTimeOffset} with provided value and size */
    static ValueVector literalDateTimeOffset(EpochDateTimeOffset value, int size)
    {
        requireNonNull(value);
        if (size == 1)
        {
            return value;
        }

        return new LiteralValueVector(ResolvedType.of(Type.DateTimeOffset), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public EpochDateTimeOffset getDateTimeOffset(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Decimal} with provided value and size */
    static ValueVector literalDecimal(Decimal value, int size)
    {
        requireNonNull(value);
        if (size == 1)
        {
            return value;
        }

        return new LiteralValueVector(ResolvedType.of(Type.Decimal), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public Decimal getDecimal(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Decimal} with provided values */
    static ValueVector literalDecimal(Decimal... values)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Decimal), values.length)
        {
            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public Decimal getDecimal(int row)
            {
                return values[row];
            }
        };
    }

    /** Create a literal vector of type {@link Column.Type#Any} with provided value and size */
    static ValueVector literalAny(int size, Object value)
    {
        requireNonNull(value);
        return new LiteralValueVector(ResolvedType.of(Type.Any), size)
        {
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

    /** Create a literal vector of type {@link Column.Type#Any} with provided values */
    static ValueVector literalAny(Object... values)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Any), values.length)
        {
            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public Object getAny(int row)
            {
                return values[row];
            }
        };
    }

    //@formatter:off
    static final Map<Type, ValueVector> ONE_SIZE_NULL_VECTOR_BY_TYPE = unmodifiableMap(new EnumMap<>(
            Map.of(
                    Type.Boolean, literalNullInternal(ResolvedType.of(Type.Boolean), 1),
                    Type.Int, literalNullInternal(ResolvedType.of(Type.Int), 1),
                    Type.Long, literalNullInternal(ResolvedType.of(Type.Long), 1),
                    Type.Float, literalNullInternal(ResolvedType.of(Type.Float), 1),
                    Type.Double, literalNullInternal(ResolvedType.of(Type.Double), 1),
                    Type.String, literalNullInternal(ResolvedType.of(Type.String), 1),
                    Type.Decimal, literalNullInternal(ResolvedType.of(Type.Decimal), 1),
                    Type.DateTime, literalNullInternal(ResolvedType.of(Type.DateTime), 1),
                    Type.DateTimeOffset, literalNullInternal(ResolvedType.of(Type.DateTimeOffset), 1)
                    )));
    //@formatter:on

    /** Create a literal null of provided value and size */
    static ValueVector literalNull(ResolvedType type, int size)
    {
        if (size == 1)
        {
            ValueVector v = ONE_SIZE_NULL_VECTOR_BY_TYPE.get(type.getType());
            if (v != null)
            {
                return v;
            }
        }
        return literalNullInternal(type, size);
    }

    private static ValueVector literalNullInternal(ResolvedType type, int size)
    {
        return new LiteralValueVector(type, size)
        {
            @Override
            public boolean isNull(int row)
            {
                return true;
            }
        };
    }

    /** Create a range vector of value between from (inclusive) and to (exclusive) */
    static ValueVector range(int from, int to)
    {
        int size = to - from;
        if (size == 0)
        {
            return empty(ResolvedType.of(Type.Int));
        }
        else if (size < 0)
        {
            throw new IllegalArgumentException("Negative range");
        }
        return new ValueVector()
        {

            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return from + row;
            }
        };
    }

    /** Create a literal int of provided value and size */
    static ValueVector literalInt(int value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Int), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal long of provided value and size */
    static ValueVector literalLong(long value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Long), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public long getLong(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal long of provided value and size */
    static ValueVector literalFloat(float value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Float), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public float getFloat(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal double of provided value and size */
    static ValueVector literalDouble(double value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Double), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public double getDouble(int row)
            {
                return value;
            }
        };
    }

    //@formatter:off
    static final List<ValueVector> TRUE_VECTORS = List.of(
            literalBooleanInternal(true, 1),
            literalBooleanInternal(true, 2),
            literalBooleanInternal(true, 3),
            literalBooleanInternal(true, 4),
            literalBooleanInternal(true, 5),
            literalBooleanInternal(true, 6),
            literalBooleanInternal(true, 7),
            literalBooleanInternal(true, 8),
            literalBooleanInternal(true, 9),
            literalBooleanInternal(true, 10)
    );
    static final List<ValueVector> FALSE_VECTORS = List.of(
            literalBooleanInternal(false, 1),
            literalBooleanInternal(false, 2),
            literalBooleanInternal(false, 3),
            literalBooleanInternal(false, 4),
            literalBooleanInternal(false, 5),
            literalBooleanInternal(false, 6),
            literalBooleanInternal(false, 7),
            literalBooleanInternal(false, 8),
            literalBooleanInternal(false, 9),
            literalBooleanInternal(false, 10)
    );
    //@formatter:on

    /** Create a literal boolean of provided value and size */
    static ValueVector literalBoolean(boolean value, int size)
    {
        if (size == 0)
        {
            return empty(ResolvedType.of(Type.Boolean));
        }

        if (size <= 10)
        {
            return value ? TRUE_VECTORS.get(size - 1)
                    : FALSE_VECTORS.get(size - 1);
        }

        return new LiteralValueVector(ResolvedType.of(Type.Boolean), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public boolean getBoolean(int row)
            {
                return value;
            }
        };
    }

    private static ValueVector literalBooleanInternal(boolean value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Boolean), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public boolean getBoolean(int row)
            {
                return value;
            }
        };
    }

    /** Create a literal string of provided value and size */
    static ValueVector literalString(String value, int size)
    {
        return literalString(UTF8String.from(value), size);
    }

    /** Create a literal string of provided value and size */
    static ValueVector literalString(UTF8String... values)
    {
        return new LiteralValueVector(ResolvedType.of(Type.String), values.length)
        {
            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public UTF8String getString(int row)
            {
                return values[row];
            }
        };
    }

    /** Create a literal string of provided value and size */
    static ValueVector literalString(UTF8String value, int size)
    {
        requireNonNull(value);
        if (size == 1)
        {
            return value;
        }

        return new LiteralValueVector(ResolvedType.of(Type.String), size)
        {
            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public UTF8String getString(int row)
            {
                return value;
            }
        };
    }

    //@formatter:off
    static final Map<Type, ValueVector> EMPTY_VECTOR_BY_TYPE = unmodifiableMap(new EnumMap<>(
            Map.of(
                    Type.Boolean, emptyInternal(ResolvedType.of(Type.Boolean)),
                    Type.Int, emptyInternal(ResolvedType.of(Type.Int)),
                    Type.Long, emptyInternal(ResolvedType.of(Type.Long)),
                    Type.Float, emptyInternal(ResolvedType.of(Type.Float)),
                    Type.Double, emptyInternal(ResolvedType.of(Type.Double)),
                    Type.String, emptyInternal(ResolvedType.of(Type.String)),
                    Type.Decimal, emptyInternal(ResolvedType.of(Type.Decimal)),
                    Type.DateTime, emptyInternal(ResolvedType.of(Type.DateTime)),
                    Type.DateTimeOffset, emptyInternal(ResolvedType.of(Type.DateTimeOffset))
                    )));
    //@formatter:on

    /** Constructs an empty {@link ValueVector} with provided type */
    static ValueVector empty(ResolvedType type)
    {
        ValueVector v = EMPTY_VECTOR_BY_TYPE.get(type.getType());
        if (v != null)
        {
            return v;
        }
        return emptyInternal(type);
    }

    private static ValueVector emptyInternal(ResolvedType type)
    {
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public int size()
            {
                return 0;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }
        };
    }

    /** Base class for literal vectors */
    abstract static class LiteralValueVector implements ValueVector
    {
        private final ResolvedType type;
        private final int size;

        LiteralValueVector(ResolvedType type, int size)
        {
            this.type = requireNonNull(type, "type");
            this.size = size;
        }

        @Override
        public ResolvedType type()
        {
            return type;
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    /** Cast provided value to boolean if possible else throws */
    static boolean toBoolean(ResolvedType type, Object v)
    {
        Object value = v;
        if (value instanceof UTF8String str)
        {
            value = str.toString();
        }
        else if (value instanceof Integer i)
        {
            return i.intValue() != 0;
        }
        else if (value instanceof Long l)
        {
            return l.longValue() != 0;
        }

        if (value instanceof String str)
        {
            // Allowed boolean strings
            // y, n
            // yes, no
            // true, false
            // 0, 1

            if ("y".equalsIgnoreCase(str)
                    || "yes".equalsIgnoreCase(str)
                    || "true".equalsIgnoreCase(str)
                    || "1".equalsIgnoreCase(str))
            {
                return true;
            }
            else if ("n".equalsIgnoreCase(str)
                    || "no".equalsIgnoreCase(str)
                    || "false".equalsIgnoreCase(str)
                    || "0".equalsIgnoreCase(str))
            {
                return false;
            }

            throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Boolean);
        }
        else if (v instanceof Boolean b)
        {
            return b.booleanValue();
        }

        throw new IllegalArgumentException("Cannot cast " + type + " to " + Type.Boolean);
    }

    /** Cast provided value to int if possible else throws */
    static int toInt(ResolvedType type, Object v)
    {
        Object value = v;
        if (value instanceof UTF8String)
        {
            value = ((UTF8String) value).toString();
        }

        if (value instanceof String)
        {
            try
            {
                return Integer.parseInt((String) value);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + value + "' to " + Type.Int);
            }
        }
        else if (value instanceof Number n)
        {
            return n.intValue();
        }
        else if (value instanceof Boolean b)
        {
            return b ? 1
                    : 0;
        }

        throw new IllegalArgumentException("Cannot cast [" + v + "] (" + type.toTypeString() + ") to " + Type.Int);
    }

    /** Cast provided value to long if possible else throws */
    static long toLong(ResolvedType type, Object v)
    {
        Object value = v;
        if (value instanceof UTF8String)
        {
            value = ((UTF8String) value).toString();
        }

        if (value instanceof String)
        {
            try
            {
                return Long.parseLong((String) value);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + value + "' to " + Type.Long);
            }
        }
        else if (value instanceof Number n)
        {
            return n.longValue();
        }
        else if (value instanceof Boolean b)
        {
            return b ? 1L
                    : 0L;
        }

        throw new IllegalArgumentException("Cannot cast type " + type.toTypeString() + " to " + Type.Long);
    }

    /** Cast provided value to float if possible else throws */
    static float toFloat(ResolvedType type, Object v)
    {
        Object value = v;
        if (value instanceof UTF8String)
        {
            value = ((UTF8String) value).toString();
        }

        if (value instanceof String str)
        {
            try
            {
                return Float.parseFloat(str);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + value + "' to " + Type.Float);
            }
        }
        else if (value instanceof Number n)
        {
            return n.floatValue();
        }
        else if (value instanceof Boolean b)
        {
            return b ? 1.0F
                    : 0.0F;
        }

        throw new IllegalArgumentException("Cannot cast type " + type.toTypeString() + " to " + Type.Float);
    }

    /** Cast provided value to double if possible else throws */
    static double toDouble(ResolvedType type, Object v)
    {
        Object value = v;
        if (value instanceof UTF8String)
        {
            value = ((UTF8String) value).toString();
        }

        if (value instanceof String)
        {
            try
            {
                return Double.parseDouble((String) value);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + value + "' to " + Type.Double);
            }
        }
        else if (value instanceof Number n)
        {
            return n.doubleValue();
        }
        else if (value instanceof Boolean b)
        {
            return b ? 1.0D
                    : 0.0D;
        }

        throw new IllegalArgumentException("Cannot cast type " + type.toTypeString() + " to " + Type.Double);
    }

    /** Return csv (tab separated) of this value vector */
    default String toCsv()
    {
        return toCsv(0);
    }

    /** Return csv (tab separated) of this value vector */
    default String toCsv(int indent)
    {
        String pad = indent <= 0 ? ""
                : StringUtils.repeat('\t', indent);

        StringBuilder sb = new StringBuilder(pad + "[");
        int size = size();
        for (int i = 0; i < size; i++)
        {
            if (isNull(i))
            {
                sb.append("null");
            }
            else
            {
                Object value;
                switch (type().getType())
                {
                    case Array:
                        value = getArray(i).toCsv(indent);
                        break;
                    case Table:
                        value = getTable(i).toCsv(indent + 1);
                        break;
                    default:
                        value = valueAsObject(i);

                        if (value instanceof UTF8String
                                || value instanceof Decimal
                                || value instanceof EpochDateTime
                                || value instanceof EpochDateTimeOffset)
                        {
                            break;
                        }

                        if (value instanceof ValueVector vv)
                        {
                            value = vv.toCsv(indent);
                        }
                        else if (value instanceof TupleVector tv)
                        {
                            value = tv.toCsv(indent + 1);
                        }
                        break;
                }
                sb.append(value);
            }
            if (i < size - 1)
            {
                sb.append("\t");
            }
        }
        sb.append("]");
        return sb.toString();
    }

}
