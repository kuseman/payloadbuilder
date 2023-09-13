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

/** Test of {@link BooleanBufferVectorBuilder} */
public class BooleanBufferVectorBuilderTest extends Assert
{
    @Test
    public void test_put()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        BooleanBufferVectorBuilder b = (BooleanBufferVectorBuilder) ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Boolean), allocator, 2);

        b.putNull();
        b.put(false);
        b.put(true);
        b.put(false);

        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, null, false, true, false), b.build());
    }

    @Test
    public void test_copy()
    {
        BufferAllocator allocator = new BufferAllocator(new AllocatorSettings().withBitSize(1));
        ABufferVectorBuilder b = ABufferVectorBuilder.getBuilder(ResolvedType.of(Column.Type.Boolean), allocator, 2);

        ValueVector source = vv(Type.Boolean, true, null, false);
        b.copy(source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, true, null, false), b.build());
    }
}
