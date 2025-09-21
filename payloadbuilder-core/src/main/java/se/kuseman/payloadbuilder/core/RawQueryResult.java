package se.kuseman.payloadbuilder.core;

import java.util.function.Function;

import se.kuseman.payloadbuilder.api.execution.TupleVector;

/** Definition of a raw query result where the client consumes the raw tuple vectors produced by the query. */
public interface RawQueryResult extends BaseQueryResult
{
    /**
     * Returns true if there are more result sets. If true then {@link #consumeResult(TupleVector)} is called with resulting vector
     */
    @Override
    boolean hasMoreResults();

    /**
     * Consumes the current result vector.
     *
     * @param consumer The consumer of the vectors. If no more vectors is wanted the consumer should return false.
     * @throws IllegalArgumentException if there are no more results
     */
    void consumeResult(Function<TupleVector, Boolean> consumer);
}
