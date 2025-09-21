package se.kuseman.payloadbuilder.core;

import se.kuseman.payloadbuilder.api.OutputWriter;

/**
 * Definition of a query result. This interface is an implementation of iterator pattern where
 * the client traverses all produced result sets in a while-mode fashion checking for {{@link #hasMoreResults()}
 * and then writing to an output via {{@link #writeResult(OutputWriter)}.
 *
 * <pre>
 *  OutputWriter writer = ......
 *  QueryResult result = ......
 *  while (result.hasMoreResult())
 *  {
 *      result.write(writer);
 *  }
 * </pre>
 */
public interface QueryResult extends BaseQueryResult
{
    /**
     * Returns true if there are more result sets. If true then {@link #writeResult(OutputWriter)} is called to write current result
     */
    @Override
    boolean hasMoreResults();

    /**
     * Write current result set to provided writer. NOTE! Caller has the responsibility to close the writer when all results has been written
     *
     * @throws IllegalArgumentException if there are no more results
     */
    void writeResult(OutputWriter writer);
}
