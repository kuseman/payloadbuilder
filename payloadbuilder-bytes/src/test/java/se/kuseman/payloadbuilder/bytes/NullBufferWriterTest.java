package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link NullBufferWriter} */
public class NullBufferWriterTest extends Assert
{
    @Test
    public void test()
    {
        Reference<NullBuffer> ref = new Reference<>();
        NullBuffer nullBuffer;
        ValueVector v;
        BytesWriter writer = new BytesWriter();

        v = VectorTestUtils.vv(Type.Int, 1, 2, 3, null, 5, 6, 7, 8, null);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // 2 bytes length
        // 3:d bit in first byte = 8
        // 1:st bit in second byte = 1
        assertArrayEquals(new byte[] { 2, 8, 1 }, writer.toBytes());
        assertEquals(3, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();

        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();
        v = VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // 0 bytes length since no null
        assertArrayEquals(new byte[] { 0 }, writer.toBytes());
        assertEquals(1, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();
        v = VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5, 6, 7, 8, null);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // 2 bytes length
        // 1:st bit in second byte = 1
        assertArrayEquals(new byte[] { 2, 0, 1 }, writer.toBytes());
        assertEquals(3, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();
        v = VectorTestUtils.vv(Type.Int, null, null, null);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // All null special. 3 = size of vector
        assertArrayEquals(new byte[] { 3 }, writer.toBytes());
        assertEquals(1, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();
        v = VectorTestUtils.vv(Type.Int, (Integer) null);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // All null special. 1 = size of vector
        assertArrayEquals(new byte[] { 1 }, writer.toBytes());
        assertEquals(1, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();
        v = VectorTestUtils.vv(Type.Int, 1);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // No null size = 0
        assertArrayEquals(new byte[] { 0 }, writer.toBytes());
        assertEquals(1, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }

        writer = new BytesWriter();

        v = VectorTestUtils.vv(Type.Int, null, null, null, null, null, null, null, null, null);
        NullBufferWriter.writeNullBuffer(writer, v, 0, v.size());

        // 2 bytes length
        // 8 bits set in first byte = -1
        // 1 bit set in second byte = 1
        assertArrayEquals(new byte[] { 9 }, writer.toBytes());
        assertEquals(1, NullBuffer.getNullBuffer(ByteBuffer.wrap(writer.toBytes()), 0, v.size(), ref));
        nullBuffer = ref.getValue();
        for (int i = 0; i < v.size(); i++)
        {
            assertEquals("Row " + i, v.isNull(i), nullBuffer.isNull(i));
        }
    }

}
