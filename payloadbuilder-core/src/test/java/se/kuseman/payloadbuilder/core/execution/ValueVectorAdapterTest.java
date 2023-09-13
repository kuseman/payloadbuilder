package se.kuseman.payloadbuilder.core.execution;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Test of {@link ValueVectorAdapter} */
public class ValueVectorAdapterTest extends Assert
{
    @Test
    public void test_all_get_methods_are_declared()
    {
        List<String> methods = Arrays.stream(Column.Type.values())
                .map(t -> "get" + t)
                .collect(toList());

        for (String method : methods)
        {
            try
            {
                ValueVector.class.getDeclaredMethod(method, int.class);
            }
            catch (NoSuchMethodException e)
            {
                fail(ValueVector.class.getSimpleName() + " should have method: " + method);
            }
        }
        for (String method : methods)
        {
            try
            {
                ValueVectorAdapter.class.getDeclaredMethod(method, int.class);
            }
            catch (NoSuchMethodException e)
            {
                fail(ValueVectorAdapter.class.getSimpleName() + " should have method: " + method);
            }
        }
    }
}
