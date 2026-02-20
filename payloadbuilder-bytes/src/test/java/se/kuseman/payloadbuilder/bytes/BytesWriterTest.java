package se.kuseman.payloadbuilder.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/** Test of {@link BytesWriter} */
class BytesWriterTest
{
    @Test
    void test_growCapacity()
    {
        // Normal case - doubles the desired capacity
        assertEquals(200, BytesWriter.growCapacity(100));
        assertEquals(2_000_000, BytesWriter.growCapacity(1_000_000));

        // Doubling still fits comfortably within an int
        assertEquals(Integer.MAX_VALUE / 2 * 2, BytesWriter.growCapacity(Integer.MAX_VALUE / 2));

        // Desired capacity itself fits in an int, but doubling it would overflow - must clamp to
        // Integer.MAX_VALUE instead of silently wrapping to a negative number (which used to make
        // ByteBuffer.allocate throw for payloads well under the actual size limit)
        long closeToMax = Integer.MAX_VALUE - 100;
        int grown = BytesWriter.growCapacity(closeToMax);
        assertEquals(Integer.MAX_VALUE, grown);
        assertTrue(grown >= closeToMax);

        // Desired capacity itself exceeds what a single buffer/array can ever hold - must fail loudly
        // instead of wrapping to a nonsensical negative allocation size
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> BytesWriter.growCapacity((long) Integer.MAX_VALUE + 1));
        assertTrue(ex.getMessage()
                .contains("Cannot grow payload buffer"), ex.getMessage());
    }

    @Test
    void test_varInt()
    {
        BytesWriter w = new BytesWriter();
        w.putVarInt(0);

        assertEquals(0, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(1, Utils.sizeOfVarInt(0));

        w = new BytesWriter();
        w.putVarInt(128);
        assertEquals(128, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(2, Utils.sizeOfVarInt(128));

        w = new BytesWriter();
        w.putVarInt(32000);
        assertEquals(32000, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(3, Utils.sizeOfVarInt(32000));

        w = new BytesWriter();
        w.putVarInt(32000000);
        assertEquals(32000000, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(4, Utils.sizeOfVarInt(32000000));

        w = new BytesWriter();
        w.putVarInt(320000000);
        assertEquals(320000000, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(5, Utils.sizeOfVarInt(320000000));

        w = new BytesWriter();
        w.putVarInt(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, Utils.readVarInt(ByteBuffer.wrap(w.toBytes()), 0));
        assertEquals(5, Utils.sizeOfVarInt(Integer.MAX_VALUE));
    }
}
