package se.kuseman.payloadbuilder.api.execution.vector;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Base definition of a value vector builder */
public interface IValueVectorBuilder
{
    /** Build a {@link ValueVector} from this builder */
    ValueVector build();

    /** Copy value vector into builder */
    void copy(ValueVector source);

    /** Put a value from provided vector into builder */
    void put(ValueVector source, int sourceRow);

    /** Put null into builder */
    void putNull();

    /** Return size of builder */
    int size();
}
