package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** Definition of a value vector. All values for one column in a {@link TupleVector} batch. */
public interface ValueVector
{
    /** Return type of vector values */
    ResolvedType type();

    /** Return true if vector value is nullable */
    default boolean isNullable()
    {
        return true;
    }

    /** Return row count of vector */
    int size();

    /**
     * Return value of provided row. NOTE! This method must not be used when checking if value is null. Use {@link ValueVector#isNull(int)}.
     */
    Object getValue(int row);

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
        else if (type == Type.Int)
        {
            return UTF8String.from(String.valueOf(getInt(row)));
        }
        else if (type == Type.Long)
        {
            return UTF8String.from(String.valueOf(getLong(row)));
        }
        else if (type == Type.Float)
        {
            return UTF8String.from(String.valueOf(getFloat(row)));
        }
        else if (type == Type.Double)
        {
            return UTF8String.from(String.valueOf(getDouble(row)));
        }
        else if (type == Type.DateTime)
        {
            return UTF8String.from(getDateTime(row).toString());
        }

        return UTF8String.from(getValue(row));
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

        return EpochDateTime.from(getValue(row));
    }

    /** Get boolean value for provided row */
    default boolean getBoolean(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row) != 0;
        }
        else if (type == Type.Long)
        {
            return getLong(row) != 0;
        }
        else if (type == Type.String)
        {
            // TODO: perform this on bytes instead when equals ignore case is implemented
            String str = getString(row).toString();

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
        else if (!(type == Type.Any
                || type == Type.Boolean))
        {
            throw new IllegalArgumentException("Cannot cast " + type + " to " + Type.Boolean);
        }

        return (boolean) getValue(row);
    }

    /** Get int value for provided row */
    default int getInt(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Long)
        {
            return (int) getLong(row);
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
        else if (type == Type.String)
        {
            String str = getString(row).toString();
            try
            {
                return Integer.parseInt(str);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Int);
            }
        }

        return ((Number) getValue(row)).intValue();
    }

    /** Get long value for provided row */
    default long getLong(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
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
        else if (type == Type.String)
        {
            String str = getString(row).toString();
            try
            {
                return Long.parseLong(str);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Long);
            }
        }

        return ((Number) getValue(row)).longValue();
    }

    /** Get float value for provided row */
    default float getFloat(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
        }
        else if (type == Type.Long)
        {
            return getLong(row);
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
        else if (type == Type.String)
        {
            String str = getString(row).toString();
            try
            {
                return Float.parseFloat(str);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Float);
            }
        }

        return ((Number) getValue(row)).floatValue();
    }

    /** Get double value for provided row */
    default double getDouble(int row)
    {
        Type type = type().getType();

        // Implicit casts
        if (type == Type.Int)
        {
            return getInt(row);
        }
        else if (type == Type.Long)
        {
            return getLong(row);
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
        else if (type == Type.String)
        {
            String str = getString(row).toString();
            try
            {
                return Double.parseDouble(str);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Double);
            }
        }

        return ((Number) getValue(row)).doubleValue();
    }

    /**
     * Return value as object include null value. Note! This method differs from {@link #getValue(int)} in that sense that this method converts the actual value to a boxed type and
     * {@link #getValue(int)} is the real accessor for the vectors actual type.
     */
    default Object valueAsObject(int row)
    {
        if (isNullable()
                && isNull(row))
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
            case String:
                return getString(row);
            case DateTime:
                return getDateTime(row);
            default:
                return getValue(row);
        }
    }

    /** Get predicate boolean, ie. null is false */
    default boolean getPredicateBoolean(int row)
    {
        // A predicate result is true if non null and true
        if (!isNullable())
        {
            return getBoolean(row);
        }
        return !isNull(row)
                && getBoolean(row);
    }

    /** Return the cardinality of this vector. Only applicable for boolean vectors. Used for predicates */
    default int getCardinality()
    {
        if (type().getType() != Type.Boolean)
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

    /** Write this value vector to provided output writer */
    default void write(OutputWriter writer, IExecutionContext context)
    {
        int size = size();
        writer.startArray();
        for (int i = 0; i < size; i++)
        {
            write(i, writer, context);
        }
        writer.endArray();
    }

    /** Write provided row to provided output writer */
    default void write(int row, OutputWriter writer, IExecutionContext context)
    {
        if (isNullable()
                && isNull(row))
        {
            writer.writeValue(null);
            return;
        }
        switch (type().getType())
        {
            case Boolean:
                writer.writeBool(getBoolean(row));
                break;
            case Double:
                writer.writeDouble(getDouble(row));
                break;
            case Float:
                writer.writeFloat(getFloat(row));
                break;
            case Int:
                writer.writeInt(getInt(row));
                break;
            case Long:
                writer.writeLong(getLong(row));
                break;
            case Any:
                Object value = getValue(row);
                // Reflectively check if the value is a vector of some sort
                if (value instanceof ValueVector)
                {
                    ((ValueVector) value).write(writer, context);
                }
                else if (value instanceof OutputWritable)
                {
                    ((OutputWritable) value).write(writer, context);
                }
                else if (value instanceof TupleVector)
                {
                    ((TupleVector) value).write(writer, context, false);
                }
                else
                {
                    writer.writeValue(value);
                }
                break;
            case String:
                // TODO: add support for UTF-8 bytes in outputwriter
                writer.writeValue(getString(row).toString());
                break;
            case DateTime:
                writer.writeValue(getDateTime(row).toString());
                break;
            case OutputWritable:
                OutputWritable structure = (OutputWritable) getValue(row);
                structure.write(writer, context);
                break;
            case TupleVector:
                TupleVector tupleVector = (TupleVector) getValue(row);
                tupleVector.write(writer, context, false);
                break;
            case ValueVector:
                ValueVector valueVector = (ValueVector) getValue(row);
                valueVector.write(writer, context);
                break;
            default:
                throw new IllegalArgumentException("Unspoorted type: " + type());
        }
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
            if (isNullable()
                    && isNull(i))
            {
                sb.append("null");
            }
            else
            {
                Object value;
                switch (type().getType())
                {
                    case Boolean:
                        value = getBoolean(i);
                        break;
                    case Double:
                        value = getDouble(i);
                        break;
                    case Float:
                        value = getFloat(i);
                        break;
                    case Int:
                        value = getInt(i);
                        break;
                    case Long:
                        value = getLong(i);
                        break;
                    case ValueVector:
                        value = ((ValueVector) getValue(i)).toCsv(indent);
                        break;
                    case TupleVector:
                        value = ((TupleVector) getValue(i)).toCsv(indent + 1);
                        break;
                    default:
                        value = getValue(i);
                        if (value instanceof ValueVector)
                        {
                            value = ((ValueVector) value).toCsv(indent);
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

    /** Create a literal null of provided type value and size */
    static ValueVector literalObject(ResolvedType type, Object value, int size)
    {
        requireNonNull(value, "use literalNull for null values");
        return new LiteralValueVector(type, size, false)
        {
            @Override
            public boolean isNull(int row)
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

    /** Create a literal null of provided type value and size */
    static ValueVector literalObject(ResolvedType type, Object... values)
    {
        return new LiteralValueVector(type, values.length, false)
        {
            @Override
            public boolean isNullable()
            {
                return true;
            }

            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public Object getValue(int row)
            {
                return values[row];
            }
        };
    }

    /** Create a literal null of provided type value and size */
    static ValueVector literalNull(ResolvedType type, int size)
    {
        return new LiteralValueVector(type, size, true)
        {
            @Override
            public boolean isNull(int row)
            {
                return true;
            }
        };
    }

    /** Create a literal int of provided type value and size */
    static ValueVector literalInt(int value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Int), size, false)
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

    /** Create a literal long of provided type value and size */
    static ValueVector literalLong(long value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Long), size, false)
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

    /** Create a literal long of provided type value and size */
    static ValueVector literalFloat(float value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Float), size, false)
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

    /** Create a literal double of provided type value and size */
    static ValueVector literalDouble(double value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Double), size, false)
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

    /** Create a literal boolean of provided type value and size */
    static ValueVector literalBoolean(boolean value, int size)
    {
        return new LiteralValueVector(ResolvedType.of(Type.Boolean), size, false)
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

    /** Base class for literal vectors */
    abstract static class LiteralValueVector implements ValueVector
    {
        private final ResolvedType type;
        private int size;
        private boolean nullable;

        LiteralValueVector(ResolvedType type, int size, boolean nullable)
        {
            this.type = requireNonNull(type, "type");
            this.size = size;
            this.nullable = nullable;
        }

        @Override
        public boolean isNullable()
        {
            return nullable;
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

        @Override
        public Object getValue(int row)
        {
            return valueAsObject(row);
        }
    }

    /** Definition of a nested value like an object/array etc. that can be lazily written to an {@link OutputWriter} */
    interface OutputWritable
    {
        /** Write this value to provided writer */
        void write(OutputWriter outputWriter, IExecutionContext context);

        @Override
        String toString();
    }
}
