package se.kuseman.payloadbuilder.api.execution.vector;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Base class that tests implementations of {@link ValueVector} and makes sure that methods are overridden. */
public abstract class ValueVectorMethodTest extends Assert
{
    private final Class<? extends ValueVector> clazz;

    public ValueVectorMethodTest(Class<? extends ValueVector> clazz)
    {
        this.clazz = clazz;
    }

    @Test
    public void test_all_get_datatype_methods_are_declared()
    {
        Arrays.stream(Column.Type.values())
                .map(t -> "get" + t)
                .forEach(m -> assertMethod(m, int.class));
    }

    @Test
    public void test_hasNulls_method()
    {
        assertMethod("hasNulls");
    }

    private void assertMethod(String method, Class<?>... types)
    {
        try
        {
            clazz.getDeclaredMethod(method, types);
        }
        catch (NoSuchMethodException e)
        {
            fail(clazz.getSimpleName() + " should have method: " + method);
        }
    }
}
