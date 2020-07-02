package com.viskan.payloadbuilder;

/** Definition of a query result */
public interface QueryResult
{
    /** Returns true if there are more result sets */
    boolean hasMoreResults();
    
    /** Returns meta data for current result set 
     * @throws IllegalArgumentException if there are no more results
     */
    QueryResultMetaData getResultMetaData();
    
    /** Write current result set to provided writer
     * @throws IllegalArgumentException if there are no more results
     */
    void writeResult(OutputWriter writer);
    
    interface QueryResultMetaData
    {
        /** Return columns for query result */
        String[] getColumns();
    }
    
}
