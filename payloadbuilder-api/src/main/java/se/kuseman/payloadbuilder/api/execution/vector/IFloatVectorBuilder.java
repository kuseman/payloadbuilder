package se.kuseman.payloadbuilder.api.execution.vector;

/** Definition of a float vector builder */
public interface IFloatVectorBuilder extends IValueVectorBuilder
{
    /**
     * Append value to builder, increasing it's size with 1
     *
     * @param value Value to append at current position.
     */
    void put(float value);
}
