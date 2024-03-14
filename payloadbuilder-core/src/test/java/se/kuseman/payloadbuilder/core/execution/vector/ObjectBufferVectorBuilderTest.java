package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link ObjectBufferVectorBuilder} */
public class ObjectBufferVectorBuilderTest extends Assert
{
    // @Test
    // public void test_dictionary_encoding()
    // {
    // // 1,2,2,2,3,3,3,3,3
    // IntBuffer values = IntBuffer.wrap(new int[] { 1, 2, 3 });
    // IntBuffer dict = IntBuffer.wrap(new int[] { 0, 1, 1, 1, 2, 2, 2, 2, 2 });
    //
    // /*
//         * @formatter:off
//         * 9 values
//         *           int     long    float    double
//         *  raw      9*4     9*8     9*4      9*8
//         *  dict     3*4     3*8
//         *           9*4     9*4
//         *           12      
//         * @formatter:on
    // */
    //
    //
    // // 9 ints => 12 int storage
    // // dictionary never good for integer vectors
    //
    // for (int i = 0; i < 9; i++)
    // {
    // System.out.println(values.get(dict.get(i)));
    // }
    // }
    //
    // @Test
    // public void test_run_end_encoding()
    // {
    // // 1,2,2,2,3,3,3,3,3
    // IntBuffer runEnds = IntBuffer.wrap(new int[] { 1, 4, 9 });
    // IntBuffer values = IntBuffer.wrap(new int[] { 1, 2, 3 });
    //
    // // 9 ints => 6 int storage
    //
    // for (int i = 0; i < 9; i++)
    // {
    //
    // int index = binarySearch(runEnds, 0, 2, i, (runEnd, key) ->
    // {
    // // a => runEnd
    // // b => key index
    //
    // // a = 5
    // // key = 2
    //
    // int r = (runEnd - 1) - key;
    // // System.out.println(runEnd + " " + key + " " + r);
    //
    // return r;
    // });
    //
    // index = (index < 0 ? Math.abs(index + 1)
    // : index);
    //
    // System.out.println(i + " " + index + " " + values.get(index));
    // }
    // }
    //
    // private static int binarySearch(IntBuffer buffer, int from, int to, int key, IntComparator c)
    // {
    // int midVal;
    // to--;
    // while (from <= to)
    // {
    // final int mid = (from + to) >>> 1;
    // midVal = buffer.get(mid);
    // final int cmp = c.compare(midVal, key);
    // if (cmp < 0)
    // from = mid + 1;
    // else if (cmp > 0)
    // to = mid - 1;
    // else
    // return mid; // key found
    // }
    // return -(from + 1);
    // }

    @Test
    public void test_fail_when_storing_wrong_type()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.String), allocator, 2);

        try
        {
            b.put(123);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Expected a String value but got: 123"));
        }
    }

    @Test
    public void test_literal_creation()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        // Test nulls
        b.put(null);
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, null, null), actual);

        // A buffer vector should not be created here
        assertFalse(actual instanceof ABufferVector);

        assertEquals(0, allocator.getStatistics()
                .getObjectAllocationCount());
        assertEquals(0, allocator.getStatistics()
                .getObjectAllocationSum());

        // Test values
        b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(UTF8String.from("hello"));
        b.put(UTF8String.from("hello"));

        actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, "hello", "hello"), actual);

        // A buffer vector should not be created here
        assertFalse(actual instanceof ABufferVector);

        assertEquals(0, allocator.getStatistics()
                .getObjectAllocationCount());
        assertEquals(0, allocator.getStatistics()
                .getObjectAllocationSum());

        // Test values
        b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(10);
        b.put(10);

        actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 10, 10), actual);

        // A buffer vector should not be created here
        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_put()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(1);
        b.put("hello");
        b.put(null);
        b.put(1F);
        b.putNull();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 1, "hello", null, 1F, null), b.build());
    }

    @Test
    public void test_intern_cache()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(new BigDecimal("10.10"));
        b.put(new String("hello"));
        b.put(new BigDecimal("10.10"));
        b.put(new String("hello"));
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, new BigDecimal("10.10"), "hello", new BigDecimal("10.10"), "hello", null), actual);

        assertSame(actual.getAny(0), actual.getAny(2));
        assertSame(actual.getAny(1), actual.getAny(3));
    }

    @Test
    public void test_strings_are_converted_to_utf8_strings()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(new String("hello"));
        b.put(UTF8String.from(new String("hello")));
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, "hello", "hello", null), actual);

        // Verify the intern cache has kicked in for different string types
        assertSame(actual.getAny(0), actual.getAny(1));

        assertTrue(actual.getAny(0) instanceof UTF8String);
        assertTrue(actual.getAny(1) instanceof UTF8String);
    }

    @Test
    public void test_strings_are_converted_to_utf8_strings_literal_mode()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ObjectBufferVectorBuilder b = (ObjectBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        b.put(new String("hello"));
        b.put(new String("hello"));

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, "hello", "hello"), actual);

        assertSame(actual.getAny(0), actual.getAny(1));
        assertTrue(actual.getAny(0) instanceof UTF8String);
        assertTrue(actual.getAny(1) instanceof UTF8String);
    }

    @Test
    public void test_copy()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Any), allocator, 2);

        ValueVector source = vv(Type.Any, 1, null, 3);

        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 1, null, 3), b.build());
    }
}
