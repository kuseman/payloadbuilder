package se.kuseman.payloadbuilder.api.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;

/** Definition of a factory that creates vector builders */
public interface IVectorFactory
{
    /** Create a mutable vector for with provided type and estimated capacity. */
    MutableValueVector getMutableVector(ResolvedType type, int estimatedCapacity);

    /** Create a builder for constructing {@link TupleVector}'s */
    ITupleVectorBuilder getTupleVectorBuilder(int estimatedCapacity);
}
