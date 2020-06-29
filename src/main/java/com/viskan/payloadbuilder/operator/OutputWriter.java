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
