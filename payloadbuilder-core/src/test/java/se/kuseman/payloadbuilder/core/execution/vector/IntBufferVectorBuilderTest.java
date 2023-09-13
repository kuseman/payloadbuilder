package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IIntVectorBuilder;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link IntBufferVectorBuilder} */
public class IntBufferVectorBuilderTest extends Assert
{
    @Test
    public void test_literal_creation()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        IIntVectorBuilder b = (IIntVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Int), allocator, 2);

        b.put(1);
        b.put(1);
        b.put(1);
        b.put(1);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 1, 1, 1, 1), actual);

        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_literal_creation_null()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        IIntVectorBuilder b = (IIntVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Int), allocator, 2);

        b.putNull();
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, null, null), actual);

        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_copy()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        IIntVectorBuilder b = (IIntVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Int), allocator, 2);

        ValueVector source = vv(Type.Int, 1, null, 2);
        b.copy(source);
        b.put(123);
        b.putNull();

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 1, null, 2, 123, null), b.build());
    }

    @Test
    public void test_put()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        IIntVectorBuilder b = (IIntVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Int), allocator, 2);

        b.putNull();
        b.put(0);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, null, 0), actual);
        assertTrue(actual instanceof ABufferVector);
    }

    @Test
    public void test_put_1()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        IIntVectorBuilder b = (IIntVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Int), allocator, 2);

        b.put(0);
        b.putNull();

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 0, null), actual);
        assertTrue(actual instanceof ABufferVector);
    }
}
