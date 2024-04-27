package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;

/** Base class for mutable vectors */
abstract class AMutableVector implements MutableValueVector
{
    protected final VectorFactory factory;
    protected int estimatedCapacity;
    protected ResolvedType type;
    protected int size;
    private BitBuffer nullBuffer;

    AMutableVector(VectorFactory factory, int estimatedCapacity, ResolvedType type)
    {
        this.factory = requireNonNull(factory, "factory");
        this.estimatedCapacity = estimatedCapacity;
        this.type = type;
    }

    @Override
    public ResolvedType type()
    {
        return type;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isNull(int row)
    {
        if (nullBuffer == null)
        {
            return false;
        }
        return nullBuffer.get(row);
    }

    @Override
    public void setNull(int row)
    {
        size = Math.max(size, row + 1);
        if (nullBuffer == null)
        {
            nullBuffer = factory.getAllocator()
                    .getBitBuffer(estimatedCapacity);
        }
        nullBuffer.put(row, true);
    }

    protected void removeNull(int row)
    {
        if (nullBuffer == null)
        {
            return;
        }
        nullBuffer.put(row, false);
    }

    /** Create a {@link MutableValueVector} from provided type and size */
    static AMutableVector getMutableVector(ResolvedType type, VectorFactory factory, int estimatedSize)
    {
        // CSOFF
        switch (type.getType())
        // CSON
        {
            case Boolean:
                return new MutableBooleanVector(factory, estimatedSize);
            case Int:
                return new MutableIntVector(factory, estimatedSize);
            case Long:
                return new MutableLongVector(factory, estimatedSize);
            case Float:
                return new MutableFloatVector(factory, estimatedSize);
            case Double:
                return new MutableDoubleVector(factory, estimatedSize);
            case Any:
            case Array:
            case DateTime:
            case DateTimeOffset:
            case Decimal:
            case Object:
            case String:
            case Table:
                return new MutableObjectVector(factory, estimatedSize, type);
            // No default case here!!!
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
