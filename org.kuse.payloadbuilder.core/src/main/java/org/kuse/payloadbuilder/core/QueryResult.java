package org.kuse.payloadbuilder.core;

/** Definition of a query result */
public interface QueryResult
{
    /** Returns true if there are more result sets */
    boolean hasMoreResults();
    
    /** Write current result set to provided writer
     * @throws IllegalArgumentException if there are no more results
     */
    void writeResult(OutputWriter writer);
    
    /** Resets the query result to initial state */
    void reset();
}
