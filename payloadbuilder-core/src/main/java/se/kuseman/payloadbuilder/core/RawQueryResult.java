package se.kuseman.payloadbuilder.core;

import java.util.function.Function;

import se.kuseman.payloadbuilder.api.catalog.Schema;
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
     * @deprecated Deprecated use {@link #consumeResult(ResultConsumer)}
     */
    @Deprecated
    default void consumeResult(Function<TupleVector, Boolean> consumer)
    {
        consumeResult(new ResultConsumer()
        {
            @Override
            public void schema(Schema schema)
            {
            }

            @Override
            public boolean consume(TupleVector vector)
            {
                return consumer.apply(vector);
            }
        });
    }

    /**
     * Consume current result.
     *
     * @throws IllegalArgumentException if there are no more results
     */
    void consumeResult(ResultConsumer consumer);

    /** Result consumer. */
    interface ResultConsumer
    {
        /**
         * The compile time schema of the current result. If the schema is empty that means that the current result is an asterisk query and schema is not known until runtime. Actual schema will be
         * available in {@link #consume(TupleVector)} method.
         */
        void schema(Schema schema);

        /** The consumer of the vectors. If no more vectors is wanted the consumer should return false. */
        boolean consume(TupleVector vector);
    }
}
