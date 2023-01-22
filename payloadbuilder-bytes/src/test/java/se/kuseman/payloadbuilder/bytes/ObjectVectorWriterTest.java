package se.kuseman.payloadbuilder.bytes;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;

/** Test of {@link ObjectVectorWriter} */
public class ObjectVectorWriterTest extends Assert
{
    @Test
    public void test_all_get_methods_are_declared_in_wrapper_vector()
    {
        List<String> methods = Arrays.stream(Column.Type.values())
                .map(t -> "get" + t)
                .collect(toList());

        for (String method : methods)
        {
            try
            {
                ObjectVectorWriter.ObjectValueVector.class.getDeclaredMethod(method, int.class);
            }
            catch (NoSuchMethodException e)
            {
                fail(ObjectVectorWriter.ObjectValueVector.class.getSimpleName() + " should have method: " + method);
            }
        }
    }
}
