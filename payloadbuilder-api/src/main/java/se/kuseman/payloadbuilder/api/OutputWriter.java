package se.kuseman.payloadbuilder.api;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Output writer that writes generated output */
public interface OutputWriter
{
    /** Flush this writer */
    default void flush()
    {
    }

    /** Close this writer */
    default void close()
    {
    }

    /**
     * Start a new a result set with provided columns.
     *
     * @param columns Columns for this result result. NOTE! Can be empty if columns are unknown until query time, then columns will be written as they come in {@link #writeFieldName(String)}.
     */
    default void initResult(String[] columns)
    {
    }

    /** Ends a result set */
    default void endResult()
    {
    }

    /** Start a new row. Called each time before a new row is to be written. */
    default void startRow()
    {
    }

    /** End row. Called when current row is complete */
    default void endRow()
    {
    }

    /** Write field name */
    void writeFieldName(String name);

    /** Write value */
    void writeValue(Object value);

    /** Write null */
    default void writeNull()
    {
        writeValue(null);
    }

    /** Write int value */
    default void writeInt(int value)
    {
        writeValue(value);
    }

    /** Write long value */
    default void writeLong(long value)
    {
        writeValue(value);
    }

    /** Write float value */
    default void writeFloat(float value)
    {
        writeValue(value);
    }

    /** Write double value */
    default void writeDouble(double value)
    {
        writeValue(value);
    }

    /** Write boolean value */
    default void writeBool(boolean value)
    {
        writeValue(value);
    }

    /** Write string value */
    default void writeString(UTF8String string)
    {
        writeValue(string.toString());
    }

    /** Write decimal value */
    default void writeDecimal(Decimal decimal)
    {
        writeValue(decimal.asBigDecimal());
    }

    /** Write datetime value */
    default void writeDateTime(EpochDateTime datetime)
    {
        writeValue(datetime.toString());
    }

    /** Write datetimeoffset value */
    default void writeDateTimeOffset(EpochDateTimeOffset datetimeOffset)
    {
        writeValue(datetimeOffset.toString());
    }

    /** Start object */
    void startObject();

    /** End object */
    void endObject();

    /** Start array */
    void startArray();

    /** End array */
    void endArray();
}
