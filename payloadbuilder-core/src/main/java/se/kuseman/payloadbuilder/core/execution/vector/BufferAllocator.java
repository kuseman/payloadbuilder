package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Standard buffer allocator used when building {@link ValueVector} and {@link TupleVector}'s. */
public class BufferAllocator
{
    private final Statistics statistics = new Statistics();
    // private final Buffers buffers;

    public BufferAllocator()
    {
        this(new AllocatorSettings());
    }

    public BufferAllocator(AllocatorSettings settings)
    {
        requireNonNull(settings);
        // this.buffers = new Buffers(settings);
    }

    /** Get int buffer */
    public IntBuffer getIntBuffer(int capacity)
    {
        statistics.intAllocationSum += capacity;

        // // Is there space in the pre-allocated array, use that first
        // if (buffers.intBuffer.length > buffers.intBufferPos + capacity)
        // {
        // IntBuffer result = IntBuffer.wrap(buffers.intBuffer, buffers.intBufferPos, capacity);
        // buffers.intBufferPos += capacity;
        // return result;
        // }

        statistics.intAllocationCount++;

        IntBuffer result = IntBuffer.allocate(capacity);
        return result;
    }

    /** Get long buffer */
    public LongBuffer getLongBuffer(int capacity)
    {
        statistics.longAllocationSum += capacity;

        // // Is there space in the pre-allocated array, use that first
        // if (buffers.longBuffer.length > buffers.longBufferPos + capacity)
        // {
        // LongBuffer result = LongBuffer.wrap(buffers.longBuffer, buffers.longBufferPos, capacity);
        // buffers.longBufferPos += capacity;
        // return result;
        // }

        statistics.longAllocationCount++;

        LongBuffer result = LongBuffer.allocate(capacity);
        return result;
    }

    /** Get float buffer */
    public FloatBuffer getFloatBuffer(int capacity)
    {
        statistics.floatAllocationSum += capacity;

        // // Is there space in the pre-allocated array, use that first
        // if (buffers.floatBuffer.length > buffers.floatBufferPos + capacity)
        // {
        // FloatBuffer result = FloatBuffer.wrap(buffers.floatBuffer, buffers.floatBufferPos, capacity);
        // buffers.floatBufferPos += capacity;
        // return result;
        // }

        statistics.floatAllocationCount++;

        FloatBuffer result = FloatBuffer.allocate(capacity);
        return result;
    }

    /** Get double buffer */
    public DoubleBuffer getDoubleBuffer(int capacity)
    {
        statistics.doubleAllocationSum += capacity;

        // // Is there space in the pre-allocated array, use that first
        // if (buffers.doubleBuffer.length > buffers.doubleBufferPos + capacity)
        // {
        // DoubleBuffer result = DoubleBuffer.wrap(buffers.doubleBuffer, buffers.doubleBufferPos, capacity);
        // buffers.doubleBufferPos += capacity;
        // return result;
        // }

        statistics.doubleAllocationCount++;

        DoubleBuffer result = DoubleBuffer.allocate(capacity);
        return result;
    }

    /** Get object buffer */
    public <T> List<T> getObjectBuffer(int capacity)
    {
        statistics.objectAllocationSum += capacity;
        statistics.objectAllocationCount++;

        return new ArrayList<>(capacity);
    }

    /** Get bit buffer */
    public BitBuffer getBitBuffer(int capacity)
    {
        // TODO: create a big BitSet and reuse ranges like int buffer
        statistics.bitAllocationSum += capacity;
        statistics.bitAllocationCount++;
        return new BitBuffer(capacity);
    }

    public Statistics getStatistics()
    {
        return statistics;
    }

    /** Statistics for allocator */
    public static class Statistics
    {
        //@formatter:off
        private static final Schema SCHEMA = Schema.of(
                Column.of("Bit count", Type.Int),
                Column.of("Bit sum", Type.Int),
                Column.of("Int count", Type.Int),
                Column.of("Int sum", Type.Int),
                Column.of("Long count", Type.Int),
                Column.of("Long sum", Type.Int),
                Column.of("Float count", Type.Int),
                Column.of("Float sum", Type.Int),
                Column.of("Double count", Type.Int),
                Column.of("Double sum", Type.Int),
                Column.of("Object count", Type.Int),
                Column.of("Object sum", Type.Int)
                );
        //@formatter:on

        private int bitAllocationCount;
        private int bitAllocationSum;

        private int intAllocationCount;
        private int intAllocationSum;

        private int longAllocationCount;
        private int longAllocationSum;

        private int floatAllocationCount;
        private int floatAllocationSum;

        private int doubleAllocationCount;
        private int doubleAllocationSum;

        private int objectAllocationCount;
        private int objectAllocationSum;

        public int getBitAllocationCount()
        {
            return bitAllocationCount;
        }

        public int getBitAllocationSum()
        {
            return bitAllocationSum;
        }

        public int getIntAllocationCount()
        {
            return intAllocationCount;
        }

        public int getIntAllocationSum()
        {
            return intAllocationSum;
        }

        public int getLongAllocationCount()
        {
            return longAllocationCount;
        }

        public int getLongAllocationSum()
        {
            return longAllocationSum;
        }

        public int getFloatAllocationCount()
        {
            return floatAllocationCount;
        }

        public int getFloatAllocationSum()
        {
            return floatAllocationSum;
        }

        public int getDoubleAllocationCount()
        {
            return doubleAllocationCount;
        }

        public int getDoubleAllocationSum()
        {
            return doubleAllocationSum;
        }

        public int getObjectAllocationCount()
        {
            return objectAllocationCount;
        }

        public int getObjectAllocationSum()
        {
            return objectAllocationSum;
        }

        /** Return a {@link ObjectVector} representation of this statistics instance */
        public ObjectVector asObject()
        {
            return new ObjectVector()
            {
                @Override
                public ValueVector getValue(int ordinal)
                {
                    switch (ordinal)
                    {
                        case 0:
                            return ValueVector.literalInt(bitAllocationCount, 1);
                        case 1:
                            return ValueVector.literalInt(bitAllocationSum, 1);
                        case 2:
                            return ValueVector.literalInt(intAllocationCount, 1);
                        case 3:
                            return ValueVector.literalInt(intAllocationSum, 1);
                        case 4:
                            return ValueVector.literalInt(longAllocationCount, 1);
                        case 5:
                            return ValueVector.literalInt(longAllocationSum, 1);
                        case 6:
                            return ValueVector.literalInt(floatAllocationCount, 1);
                        case 7:
                            return ValueVector.literalInt(floatAllocationSum, 1);
                        case 8:
                            return ValueVector.literalInt(doubleAllocationCount, 1);
                        case 9:
                            return ValueVector.literalInt(doubleAllocationSum, 1);
                        case 10:
                            return ValueVector.literalInt(objectAllocationCount, 1);
                        case 11:
                            return ValueVector.literalInt(objectAllocationSum, 1);
                        default:
                            throw new IllegalArgumentException("Invaild ordinal");
                    }
                }

                @Override
                public Schema getSchema()
                {
                    return SCHEMA;
                }
            };
        }
    }

    /** Class with initial buffer settings */
    public static class AllocatorSettings
    {
        // private static final int DEFAULT_INT_SIZE = 0;
        // private static final int DEFAULT_LONG_SIZE = 0;
        // private static final int DEFAULT_FLOAT_SIZE = 0;
        // private static final int DEFAULT_DOUBLE_SIZE = 0;
        // private static final int DEFAULT_BIT_SIZE = 0;

        // /** Size of integer buffer */
        // private int intSize = DEFAULT_INT_SIZE;
        // /** Size of long buffer */
        // private int longSize = DEFAULT_LONG_SIZE;
        // /** Size of float buffer */
        // private int floatSize = DEFAULT_FLOAT_SIZE;
        // /** Size of float buffer */
        // private int doubleSize = DEFAULT_DOUBLE_SIZE;
        // /** Size of bit buffer */
        // @SuppressWarnings("unused")
        // private int bitSize = DEFAULT_BIT_SIZE;

        /** Init settings with Int buffer size */
        public AllocatorSettings withIntSize(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Invalid int buffer size: " + size);
            }
            // this.intSize = size;
            return this;
        }

        /** Init settings with Long buffer size */
        public AllocatorSettings withLongSize(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Invalid long buffer size: " + size);
            }
            // this.longSize = size;
            return this;
        }

        /** Init settings with Float buffer size */
        public AllocatorSettings withFloatSize(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Invalid float buffer size: " + size);
            }
            // this.floatSize = size;
            return this;
        }

        /** Init settings with Double buffer size */
        public AllocatorSettings withDoubleSize(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Invalid double buffer size: " + size);
            }
            // this.doubleSize = size;
            return this;
        }

        /** Init settings with Bit buffer size */
        public AllocatorSettings withBitSize(int size)
        {
            if (size <= 0)
            {
                throw new IllegalArgumentException("Invalid bit buffer size: " + size);
            }
            // this.bitSize = size;
            return this;
        }
    }

    // static class Buffers
    // {
    // private final int[] intBuffer;
    // private int intBufferPos;
    //
    // private final long[] longBuffer;
    // private int longBufferPos;
    //
    // private final float[] floatBuffer;
    // private int floatBufferPos;
    //
    // private final double[] doubleBuffer;
    // private int doubleBufferPos;
    //
    // Buffers(AllocatorSettings settings)
    // {
    // this.intBuffer = new int[settings.intSize];
    // this.longBuffer = new long[settings.longSize];
    // this.floatBuffer = new float[settings.floatSize];
    // this.doubleBuffer = new double[settings.doubleSize];
    // }
    // }
}
