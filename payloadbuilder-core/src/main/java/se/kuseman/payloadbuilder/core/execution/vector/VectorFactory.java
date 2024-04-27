package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.vector.ITupleVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorFactory;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;

/** Default implementation of {@link IVectorFactory} */
public class VectorFactory implements IVectorFactory
{
    private final BufferAllocator allocator;
    // private final boolean trace;
    // private final List<AMutableVector> vectors;
    private final Map<Column.Type, Deque<AMutableVector>> vectorsByType = new EnumMap<>(Column.Type.class);

    // public VectorFactory(BufferAllocator allocator)
    // {
    // this(allocator, false);
    // }

    public VectorFactory(BufferAllocator allocator)
    {
        // this.trace = trace;
        this.allocator = requireNonNull(allocator, "allocator");
        // this.vectors = new ArrayList<>();
        Deque<AMutableVector> objectList = new ArrayDeque<>();
        for (Column.Type type : Column.Type.values())
        {
            if (type.isPrimitive())
            {
                vectorsByType.put(type, new ArrayDeque<>());
            }
            else
            {
                vectorsByType.put(type, objectList);
            }
        }
        // this.vectors = trace ? new ArrayList<>()
        // : emptyList();
    }

    public BufferAllocator getAllocator()
    {
        return allocator;
    }

    @Override
    public MutableValueVector getMutableVector(ResolvedType type, int estimatedCapacity)
    {
        AMutableVector vector;
        vector = AMutableVector.getMutableVector(type, this, estimatedCapacity);
        return vector;
    }

    @Override
    public ITupleVectorBuilder getTupleVectorBuilder(int estimatedCapacity)
    {
        return new TupleVectorBuilder(this, estimatedCapacity);
    }

    void returnVector(AMutableVector vector)
    {
        // System.out.println("Returning: " + vector.hashCode() + " type: " + vector.type() + ", refCount: " + vector.refCount + ", size: " + vector.size);
        // vector.reset();

        // This vector is already returned to pool
        // Don't add it again
        // This happens when the same vector exists multiple times in another vector.
        // Ie. projecting the same column etc.
        // if (!vector.leased)
        // {
        // return;
        // }

        // vector.leased = false;
        // vector.refCount--;

        // This vector is already returned to pool
        // Don't add it again
        // This happens when the same vector exists multiple times in another vector.
        // Ie. projecting the same column etc.
        // if (vector.refCount < 0)
        // {
        // vector.refCount = 0;
        // return;
        // }
        //
        // if (vector.refCount == 0)
        // {
        // Deque<AMutableVector> deque = vectorsByType.get(vector.type()
        // .getType());
        // // System.out.println("Returning: " + vector.hashCode() + " type: " + vector.type() + ", refCount: " + vector.refCount + ", size: " + vector.size);
        // deque.offer(vector);
        // vector.refCount = 0;
        // }
    }

    public void printAllocationInformation()
    {
        // if (!trace)
        // {
        // return;
        // }
        // for (AMutableVector v : vectors)
        // {
        // if (v.refCount > 0)
        // {
        // System.out.println("Refcount " + v.refCount);
        // System.out.println("Stack: " + Arrays.toString(v.allocationStack));
        //
        // }
        //
        // }
    }

}
