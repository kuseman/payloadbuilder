package org.kuse.payloadbuilder.core;

/** Output writer that writes generated output */
public interface OutputWriter
{
    /**
     * Start a new a result set with provided columns.
     *
     * @param columns Columns for this result result. NOTE! Can be null if columns are unknown (select *).
     */
    default void initResult(String[] columns)
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

    /** Start object */
    void startObject();

    /** End object */
    void endObject();

    /** Start array */
    void startArray();

    /** End array */
    void endArray();
}
