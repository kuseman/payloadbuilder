package se.kuseman.payloadbuilder.api.execution.vector;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Definition of a mutable value vector. Used when building new vectors of a certain type */
public interface MutableValueVector extends ValueVector
{
    /** Set null at provided row */
    void setNull(int row);

    /** Set boolean value at provided row. */
    default void setBoolean(int row, boolean value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setBoolean");
    }

    /** Set integer value to provided row. */
    default void setInt(int row, int value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setInt");
    }

    /** Set long value to provided row. */
    default void setLong(int row, long value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setLong");
    }

    /** Set float value to provided row. */
    default void setFloat(int row, float value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setFloat");
    }

    /** Set double value to provided row. */
    default void setDouble(int row, double value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setDouble");
    }

    /** Set decimal to provided row. */
    default void setDecimal(int row, Decimal value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setDecimal");
    }

    /** Set string to provided row. */
    default void setString(int row, UTF8String value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setString");
    }

    /** Set date time to provided row. */
    default void setDateTime(int row, EpochDateTime value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setDateTime");
    }

    /** Set date time offset to provided row. */
    default void setDateTimeOffset(int row, EpochDateTimeOffset value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setDateTimeOffset");
    }

    /** Set array value to provided row. */
    default void setArray(int row, ValueVector value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setArray");
    }

    /** Set any value to provided row. */
    default void setObject(int row, ObjectVector value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setObject");
    }

    /** Set any value to provided row. */
    default void setTable(int row, TupleVector value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setTable");
    }

    /** Set any value to provided row. */
    default void setAny(int row, Object value)
    {
        throw new IllegalArgumentException("Check implementation of MutableValueVector: " + getClass() + " for setAny");
    }

    /**
     * Copy source vector into this vector.
     *
     * @param startRow Start row in this vector to put rows
     * @param source Source vector to copy data from
     */
    default void copy(int startRow, ValueVector source)
    {
        copy(startRow, source, 0, source.size());
    }

    /**
     * Copy source vector into this vector.
     *
     * @param startRow Start row in this vector to put rows
     * @param source Source vector to copy data from
     * @param sourceRow Start row in source vector
     */
    default void copy(int startRow, ValueVector source, int sourceRow)
    {
        copy(startRow, source, sourceRow, 1);
    }

    /**
     * Copy source vector into this vector.
     *
     * @param startRow Start row in this vector to put rows
     * @param source Source vector to copy data from
     * @param sourceRow Start row in source vector
     * @param length Number of rows to copy from source vector
     */
    void copy(int startRow, ValueVector source, int sourceRow, int length);
}
