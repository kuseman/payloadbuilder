package se.kuseman.payloadbuilder.core.execution.vector;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator.AllocatorSettings;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link MutableObjectVector} */
public class MutableObjectVectorTest extends Assert
{
    @Test
    public void test_fail_when_storing_wrong_type()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.String), 2);

        assertFalse(b.hasNulls());

        try
        {
            b.setInt(0, 123);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Check implementation of MutableValueVector: class se.kuseman.payloadbuilder.core.execution.vector.MutableObjectVector for setInt"));
        }
    }

    @Test
    public void test_literal_creation()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        assertFalse(b.hasNulls());

        // Test nulls
        b.setNull(0);
        b.setNull(1);

        assertTrue(b.hasNulls());

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, null, null), b);

        assertEquals(1, factory.getAllocator()
                .getStatistics()
                .getObjectAllocationCount());
        assertEquals(2, factory.getAllocator()
                .getStatistics()
                .getObjectAllocationSum());

        // Test values
        b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        b.setAny(0, UTF8String.from("hello"));
        b.setAny(1, UTF8String.from("hello"));

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, "hello", "hello"), b);

        assertEquals(2, factory.getAllocator()
                .getStatistics()
                .getObjectAllocationCount());
        assertEquals(4, factory.getAllocator()
                .getStatistics()
                .getObjectAllocationSum());
    }

    @Test
    public void test_put()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        b.setAny(0, 1);
        b.setAny(1, "hello");
        b.setNull(2);
        b.setAny(3, 1F);
        b.setNull(4);

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 1, "hello", null, 1F, null), b);
    }

    @Test
    public void test_put_1()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        b.setNull(0);
        b.setAny(0, 1);

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 1), b);
    }

    @Test
    public void test_strings_are_converted_to_utf8_strings()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        b.setAny(0, new String("hello"));
        b.setAny(1, UTF8String.from(new String("hello")));
        b.setNull(2);

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, "hello", "hello", null), b);

        assertTrue(b.getAny(0) instanceof UTF8String);
        assertTrue(b.getAny(1) instanceof UTF8String);
    }

    @Test
    public void test_copy()
    {
        VectorFactory factory = new VectorFactory(new BufferAllocator(new AllocatorSettings().withBitSize(1)));
        MutableValueVector b = factory.getMutableVector(ResolvedType.of(Column.Type.Any), 2);

        ValueVector source = vv(Type.Any, 1, null, 3);

        b.copy(0, source);

        VectorTestUtils.assertVectorsEquals(vv(Type.Any, 1, null, 3), b);
    }
}
