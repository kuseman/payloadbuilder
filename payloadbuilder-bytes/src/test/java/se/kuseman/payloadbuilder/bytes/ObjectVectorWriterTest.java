package se.kuseman.payloadbuilder.bytes;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Test of {@link ObjectVectorWriter} */
class ObjectVectorWriterTest
{
    @Test
    void test_all_get_methods_are_declared_in_wrapper_vector()
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

    /**
     * Regression test: {@link ObjectVectorWriter.ObjectValueVector#isNull(int)} used to dereference {@code wrapped.getObject(row)} unconditionally, even when the whole object at that row was itself
     * null - throwing a NullPointerException instead of writing the row as null. Only reproduces when a null row is *not* the last one written (a trailing null never reaches the failing code path via
     * the null-buffer's flush-on-last-byte check), so this deliberately puts the null in the middle.
     */
    @Test
    void test_write_object_vector_with_a_null_row_not_last()
    {
        Schema innerSchema = Schema.of(Column.of("int_0", Type.Int));

        ObjectVector obj0 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 10))));
        ObjectVector obj2 = ObjectVector.wrap(TupleVector.of(innerSchema, List.of(vv(Type.Int, 30))));

        ValueVector v = vv(ResolvedType.object(innerSchema), obj0, null, obj2);

        byte[] bytes = PayloadWriter.write(v);
        ValueVector actual = PayloadReader.read(bytes);

        assertVectorsEquals(v, actual);
    }
}
