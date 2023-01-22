package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.ILongVectorBuilder;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link LongBufferVectorBuilder} */
public class LongBufferVectorBuilderTest extends Assert
{
    @Test
    public void test_literal_creation()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ILongVectorBuilder b = (ILongVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        b.put(1);
        b.put(1);
        b.put(1);
        b.put(1);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 1, 1, 1, 1), actual);

        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_literal_creation_null()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ILongVectorBuilder b = (ILongVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        b.putNull();
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, null, null), actual);

        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_put()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ILongVectorBuilder b = (ILongVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        ValueVector source = vv(Type.Long, 1L, null, 2L);
        b.put(source, 0);
        b.put(source, 1);
        b.putNull();
        b.put(10L);
        b.putNull();

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 1L, null, null, 10L, null), b.build());
    }

    @Test
    public void test_put_1()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ILongVectorBuilder b = (ILongVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        b.putNull();
        b.put(0);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, null, 0), actual);
        assertTrue(actual instanceof ABufferVector);
    }

    @Test
    public void test_put_2()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ILongVectorBuilder b = (ILongVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        b.put(0);
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 0, null), actual);
        assertTrue(actual instanceof ABufferVector);
    }

    @Test
    public void test_copy()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Long), allocator, 2);

        ValueVector source = vv(Type.Long, 1L, null, 2L);
        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 1L, null, 2L), b.build());
    }
}
