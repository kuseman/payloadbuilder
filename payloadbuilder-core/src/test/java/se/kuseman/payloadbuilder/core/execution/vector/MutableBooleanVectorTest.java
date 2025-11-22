package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link MutableBooleanVector} */
class MutableBooleanVectorTest
{
    @Test
    void test_put()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Boolean), 2);

        b.setNull(0);
        b.setBoolean(1, false);
        b.setBoolean(2, true);
        b.setBoolean(3, false);

        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, null, false, true, false), b);
    }

    @Test
    void test_put_1()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Boolean), 2);

        b.setNull(0);
        b.setBoolean(0, false);

        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, false), b);
    }

    @Test
    void test_copy()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Boolean), 2);

        ValueVector source = vv(Type.Boolean, true, null, false);
        b.copy(0, source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, true, null, false), b);
    }
}
