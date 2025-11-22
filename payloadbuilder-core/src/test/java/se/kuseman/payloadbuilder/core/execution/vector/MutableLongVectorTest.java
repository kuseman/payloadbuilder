package se.kuseman.payloadbuilder.core.execution.vector;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link MutableLongVector} */
class MutableLongVectorTest
{
    @Test
    void test_literal_creation()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Long), 2);

        b.setLong(0, 1);
        b.setLong(1, 1);
        b.setLong(2, 1);
        b.setLong(3, 1);

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 1, 1, 1, 1), b);
    }

    @Test
    void test_literal_creation_null()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Long), 2);

        b.setNull(0);
        b.setNull(1);

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, null, null), b);
    }

    @Test
    void test_copy()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Long), 2);

        ValueVector source = vv(Type.Int, 1, null, 2);
        b.copy(0, source);
        b.setLong(b.size(), 123);
        b.setNull(b.size());

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 1, null, 2, 123, null), b);
    }

    @Test
    void test_put()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Long), 2);

        b.setNull(0);
        b.setLong(1, 0);

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, null, 0), b);
        assertTrue(b instanceof MutableLongVector);
    }

    @Test
    void test_put_1()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Long), 2);

        b.setNull(0);
        b.setLong(0, 0);

        VectorTestUtils.assertVectorsEquals(vv(Type.Long, 0), b);
    }
}
