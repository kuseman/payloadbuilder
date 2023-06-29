package se.kuseman.payloadbuilder.api.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Definition of a factory that creates vector builders */
public interface IVectorBuilderFactory
{
    /** Create a {@link IBooleanVectorBuilder} with provided estimated capacity */
    IBooleanVectorBuilder getBooleanVectorBuilder(int estimatedCapacity);

    /** Create a {@link IIntVectorBuilder} with provided estimated capacity */
    IIntVectorBuilder getIntVectorBuilder(int estimatedCapacity);

    /** Create a {@link ILongVectorBuilder} with provided estimated capacity */
    ILongVectorBuilder getLongVectorBuilder(int estimatedCapacity);

    /** Create a {@link IFloatVectorBuilder} with provided estimated capacity */
    IFloatVectorBuilder getFloatVectorBuilder(int estimatedCapacity);

    /** Create a {@link IDoubleVectorBuilder} with provided estimated capacity */
    IDoubleVectorBuilder getDoubleVectorBuilder(int estimatedCapacity);

    /** Cerate a {@link IObjectVectorBuilder} with provided type and estimated capacity */
    IObjectVectorBuilder getObjectVectorBuilder(ResolvedType type, int estimatedCapacity);

    /** Create a {@link IValueVectorBuilder} for provided type and estimated capacity */
    IValueVectorBuilder getValueVectorBuilder(ResolvedType type, int estimatedCapacity);
}
