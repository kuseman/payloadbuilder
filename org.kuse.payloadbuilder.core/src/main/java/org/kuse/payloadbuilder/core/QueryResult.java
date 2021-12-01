package org.kuse.payloadbuilder.core;

/** Definition of a query result */
public interface QueryResult
{
    /**
     * Returns true if there are more result sets. If true then {@link #writeResult(OutputWriter)} is supposed to be called to write current result
     */
    boolean hasMoreResults();

    /**
     * Write current result set to provided writer
     *
     * @throws IllegalArgumentException if there are no more results
     */
    void writeResult(OutputWriter writer);
}
