package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Byte writer. */
class BytesWriter
{
    private ByteBuffer buffer;

    BytesWriter()
    {
        buffer = ByteBuffer.allocate(1048)
                .order(PayloadReader.BYTE_ORDER);
    }

    int position()
    {
        return buffer.position();
    }

    /** Set position of writer */
    void position(int position)
    {
        ensureCapacity(position);
        buffer.position(position);
    }

    /** Put int at current position and increment position. */
    void putInt(int value)
    {
        ensureCapacity(buffer.position() + 4);
        buffer.putInt(value);
    }

    /** Put int at provided position. */
    void putInt(int position, int value)
    {
        ensureCapacity(position + 4);
        buffer.putInt(position, value);
    }

    /** Put float at current position and increment position. */
    void putFloat(float value)
    {
        ensureCapacity(buffer.position() + 4);
        buffer.putFloat(value);
    }

    /** Put long at current position and increment position. */
    void putLong(long value)
    {
        ensureCapacity(buffer.position() + 8);
        buffer.putLong(value);
    }

    /** Put double at current position and increment position. */
    void putDouble(double value)
    {
        ensureCapacity(buffer.position() + Double.BYTES);
        buffer.putDouble(value);
    }

    /** Return byte at provided position */
    byte getByte(int position)
    {
        return buffer.get(position);
    }

    /** Put byte at current position and increment position. */
    void putByte(byte b)
    {
        ensureCapacity(buffer.position() + 1);
        buffer.put(b);
    }

    /** Put bytes at current position and increment position. */
    void putBytes(byte[] b)
    {
        ensureCapacity(buffer.position() + b.length);
        buffer.put(b);
    }

    /** Put var int at current position and increment position. */
    void putVarInt(int value)
    {
        if (value < 0)
        {
            throw new IllegalArgumentException("negative value");
        }
        //@formatter:off
        //CSOFF
        if(value > 0x0FFFFFFF || value < 0) { putByte((byte)(0x80 | ((value >>> 28)))); }
        if(value > 0x1FFFFF || value < 0)   { putByte((byte)(0x80 | ((value >>> 21) & 0x7F))); }
        if(value > 0x3FFF || value < 0)     { putByte((byte)(0x80 | ((value >>> 14) & 0x7F))); }
        if(value > 0x7F || value < 0)       { putByte((byte)(0x80 | ((value >>>  7) & 0x7F))); }
        //CSON
        //@formatter:on

        putByte((byte) (value & 0x7F));
    }

    /** Get a copy of the buffer as byte array. */
    byte[] toBytes()
    {
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    /** Reset buffer for reuse. */
    void reset()
    {
        buffer.clear();
    }

    void ensureCapacity(long desiredCapacity)
    {
        if (buffer.capacity() < desiredCapacity)
        {
            ByteBuffer newBuffer = ByteBuffer.allocate(growCapacity(desiredCapacity));
            int position = buffer.position();
            buffer.position(0);
            newBuffer.put(buffer);
            newBuffer.position(position);
            buffer = newBuffer;
        }
    }

    /**
     * Compute the capacity to grow to for the given desired capacity. Doubles the desired capacity to amortize future growth, clamped to {@link Integer#MAX_VALUE} since a single buffer/array can't be
     * bigger than that - all arithmetic here is done in {@code long} precision specifically so that doubling a desired capacity close to {@link Integer#MAX_VALUE} doesn't silently wrap around to a
     * negative number, which used to make {@link ByteBuffer#allocate(int)} throw for payloads well under the actual size limit.
     */
    static int growCapacity(long desiredCapacity)
    {
        if (desiredCapacity > Integer.MAX_VALUE)
        {
            throw new IllegalStateException("Cannot grow payload buffer beyond " + Integer.MAX_VALUE + " bytes, requested capacity: " + desiredCapacity);
        }

        long doubledCapacity = desiredCapacity * 2;
        return doubledCapacity > Integer.MAX_VALUE ? Integer.MAX_VALUE
                : (int) doubledCapacity;
    }
}
