package se.kuseman.payloadbuilder.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/** Test of {@link BytesWriter} */
class BytesWriterTest
{
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
