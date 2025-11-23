package se.kuseman.payloadbuilder.api.execution;

/** Definition of a vector writer. Writes {@link TupleVector}'s to an outputstream. */
public interface VectorWriter extends AutoCloseable
{
    /**
     * Write provided vector to stream.
     *
     * @param vector Vector to write to stream.
     */
    void write(TupleVector vector);
}
