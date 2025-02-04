package se.kuseman.payloadbuilder.api.execution.vector;

import java.util.BitSet;

import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Definition of tuple vector builder. This is used to build {@link TupleVector}'s by appending source vectors with filters etc.
 */
public interface ITupleVectorBuilder
{
    /** Build a resulting tuple vector from this builder */
    TupleVector build();

    /** Append a source tuple vector along with a boolean filter to this builder. */
    void append(TupleVector source, ValueVector filter);

    /** Append a source tuple vector along with a boolean filter to this builder. */
    void append(TupleVector source, BitSet bitSet);

    /** Append a source tuple vector to this builder. */
    void append(TupleVector source);

    /** Append an outer/inner vector combo as a populating vector. */
    void appendPopulate(TupleVector currentOuter, TupleVector concatOfInner, ValueVector filter, String populateAlias);
}
