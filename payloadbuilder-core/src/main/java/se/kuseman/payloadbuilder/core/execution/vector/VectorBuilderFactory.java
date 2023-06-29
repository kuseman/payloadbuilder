package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IDoubleVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IFloatVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IIntVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.ILongVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorBuilderFactory;

/** Default implementation of {@link IVectorBuilderFactory} */
public class VectorBuilderFactory implements IVectorBuilderFactory
{
    private final BufferAllocator allocator;

    public VectorBuilderFactory(BufferAllocator allocator)
    {
        this.allocator = requireNonNull(allocator, "allocator");
    }

    @Override
    public IBooleanVectorBuilder getBooleanVectorBuilder(int estimatedCapacity)
    {
        return new BooleanBufferVectorBuilder(allocator, estimatedCapacity);
    }

    @Override
    public IIntVectorBuilder getIntVectorBuilder(int estimatedCapacity)
    {
        return new IntBufferVectorBuilder(allocator, estimatedCapacity);
    }

    @Override
    public ILongVectorBuilder getLongVectorBuilder(int estimatedCapacity)
    {
        return new LongBufferVectorBuilder(allocator, estimatedCapacity);
    }

    @Override
    public IFloatVectorBuilder getFloatVectorBuilder(int estimatedCapacity)
    {
        return new FloatBufferVectorBuilder(allocator, estimatedCapacity);
    }

    @Override
    public IDoubleVectorBuilder getDoubleVectorBuilder(int estimatedCapacity)
    {
        return new DoubleBufferVectorBuilder(allocator, estimatedCapacity);
    }

    @Override
    public IObjectVectorBuilder getObjectVectorBuilder(ResolvedType type, int estimatedCapacity)
    {
        if (type.getType()
                .isPrimitive())
        {
            throw new IllegalArgumentException("Object vector builder cannot be used with primitive types");
        }

        return (IObjectVectorBuilder) ABufferVectorBuilder.getBuilder(type, allocator, estimatedCapacity);
    }

    @Override
    public IValueVectorBuilder getValueVectorBuilder(ResolvedType type, int estimatedCapacity)
    {
        return ABufferVectorBuilder.getBuilder(type, allocator, estimatedCapacity);
    }
}
