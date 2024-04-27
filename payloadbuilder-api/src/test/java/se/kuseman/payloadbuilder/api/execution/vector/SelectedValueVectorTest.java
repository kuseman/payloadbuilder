package se.kuseman.payloadbuilder.api.execution.vector;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;

/** Test of {@link SelectedValueVector} */
public class SelectedValueVectorTest extends Assert
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
                SelectedValueVector.class.getDeclaredMethod(method, int.class);
            }
            catch (NoSuchMethodException e)
            {
                fail(SelectedValueVector.class.getSimpleName() + " should have method: " + method);
            }
        }
    }
}
