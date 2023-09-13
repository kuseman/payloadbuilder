package se.kuseman.payloadbuilder.api.execution.vector;

/** Definition of a object vector builder. Builds vectors with types that is not primitive values */
public interface IObjectVectorBuilder extends IValueVectorBuilder
{
    /**
     * Append value to builder, increasing it's size with 1
     *
     * @param value Value to append at current position. Accepts null
     */
    void put(Object value);
}
