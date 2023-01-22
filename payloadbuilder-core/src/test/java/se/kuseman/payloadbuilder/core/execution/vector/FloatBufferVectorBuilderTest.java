package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link FloatBufferVectorBuilder} */
public class FloatBufferVectorBuilderTest extends Assert
{
    @Test
    public void test_copy()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Float), allocator, 2);

        ValueVector source = vv(Type.Float, 1.0F, null, 2.0F);
        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 1.0F, null, 2.0F), b.build());
    }

    @Test
    public void test_copy_2()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Float), allocator, 2);

        ValueVector source = vv(Type.Float, (Float) null);
        b.copy(source);

        source = vv(Type.Float, 1.0F);
        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Float, null, 1.0F), b.build());
    }

    @Test
    public void test_copy_3()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Float), allocator, 2);

        ValueVector source = vv(Type.Float, 1.0F);
        b.copy(source);

        source = vv(Type.Float, (Float) null);
        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 1.0F, null), b.build());
    }

    @Test
    public void test_literal_creation()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Float), allocator, 2);

        ValueVector source = vv(Type.Float, 1.0F, 1.0F, 1.0F);
        b.copy(source);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 1.0F, 1.0F, 1.0F), actual);
        assertFalse(actual instanceof ABufferVector);
    }

    @Test
    public void test_literal_null()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Float), allocator, 2);

        ValueVector source = vv(Type.Float, null, null, null);
        b.copy(source);

        ValueVector actual = b.build();

        VectorTestUtils.assertVectorsEquals(vv(Type.Float, null, null, null), actual);
        assertFalse(actual instanceof ABufferVector);
    }
}
