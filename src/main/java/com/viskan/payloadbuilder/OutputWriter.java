package com.viskan.payloadbuilder;

/** Output writer that writes generated output */
public interface OutputWriter
{
    /** Start a new row. Called each time before a new row is to be written. */
    default void startRow()
    {}
    
    /** End row. Called when current row is complete */
    default void endRow()
    {}
    
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
    
    /*
     * {
     *   "int": 1,
     *   "bool": true,
     *   "arr": [1,2,3,4,5],
     *   "objArr": [
     *     {
     *       ...
     *     },
     *     {
     *       ...
     *     }
     *   ]
     * }
     * 
     * RowSet rs;
     * Object obj;
     * obj = rs.getObject(0)
     * obj = rs.getObject(1)
     * 
     * RowSet arRs = rs.getArray(2);
     * 
     * 
     */
    
    
    
    
}
