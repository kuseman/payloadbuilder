package com.viskan.payloadbuilder.operator;

/** Output writer that writes generated output */
public interface OutputWriter
{
    /** Write field name */
    void writeFieldName(String name);
    
    /** Write value */
    void writeValue(Object value);

    /** Start object with name */
    void startObject();

    /** End object */
    void endObject();

    /** Start array with name */
    void startArray();

    /** End array */
    void endArray();
}
