package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.DatasourceOptions;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link OpenJsonFunction} */
public class OpenJsonFunctionTest extends APhysicalPlanTest
{
    private final TableFunctionInfo f = SystemCatalog.get()
            .getTableFunction("openjson");

    @Test
    public void test_empty_on_null()
    {
        assertEquals(Arity.ONE, f.arity());
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("null")), new DatasourceOptions(emptyList()));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_empty_object()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{}'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(Schema.EMPTY, next.getSchema());

            rowCount += next.getRowCount();
        }

        try
        {
            it.next();
            fail("Should throw NoSuchElementException");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_object()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{ \"key\": 123 }'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Column.Type.Any)), List.of(VectorTestUtils.vv(Column.Type.Any, 123))), next);
            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_empty_array()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[]'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(Schema.EMPTY, next.getSchema());

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(0, rowCount);
    }

    @Test
    public void test_array_object()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456}]'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, 123, null),
                            VectorTestUtils.vv(Column.Type.Any, null, 456)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_array_object_batch_size()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456}]'")),
                new DatasourceOptions(List.of(new Option(DatasourceOptions.BATCH_SIZE, intLit(1)))));

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            batchCount++;

            if (batchCount == 1)
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, 123)
                                )), next);
                //@formatter:on
            }
            else
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key2", Column.Type.Any)), List.of(
                                VectorTestUtils.vv(Column.Type.Any, 456)
                                )), next);
                //@formatter:on
            }
            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertEquals(2, batchCount);
    }

    @Test
    public void test_array_object_empty_row()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456},{}]'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, 123, null, null),
                            VectorTestUtils.vv(Column.Type.Any, null, 456, null)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    @Test
    public void test_pointer_not_found()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456},{}]'")),
                new DatasourceOptions(List.of(new Option(OpenJsonFunction.JSONPATH, e("'/none'")))));

        assertFalse(it.hasNext());
        it.close();
    }

    @Test
    public void test_pointer_to_array()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{\"rows\": [{ \"key\": 123 },{ \"key2\": 456},{}], \"otherData\": { \"key\":234 }}'")),
                new DatasourceOptions(List.of(new Option(OpenJsonFunction.JSONPATH, e("'/rows'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.Any), Column.of("key2", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, 123, null, null),
                            VectorTestUtils.vv(Column.Type.Any, null, 456, null)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    @Test
    public void test_pointer_to_object()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{\"otherData\": { \"key\":234 }, \"row\": { \"key\": 123 }}'")),
                new DatasourceOptions(List.of(new Option(OpenJsonFunction.JSONPATH, e("'/row'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, 123)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_pointer_to_empty_object()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{\"otherData\": { \"key\":234 }, \"row\": {}}'")),
                new DatasourceOptions(List.of(new Option(OpenJsonFunction.JSONPATH, e("'/row'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            assertEquals(Schema.EMPTY, next.getSchema());
            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    @Test
    public void test_pointer_to_empty_array()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'{\"otherData\": { \"key\":234 }, \"rows\": []}'")),
                new DatasourceOptions(List.of(new Option(OpenJsonFunction.JSONPATH, e("'/rows'")))));

        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        it.close();
    }

    @Test
    public void test_array_mixed_values()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456},{}, true]'")), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("Value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, Map.of("key", 123), Map.of("key2", 456), Map.of(), true)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(4, rowCount);
    }

    @Test
    public void test_array_broken_json_last()
    {
        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(e("'[{ \"key\": 123 },{ \"key2\": 456},{}, true'")), new DatasourceOptions(emptyList()));

        try
        {
            it.hasNext();
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Error reading JSON"));
        }
    }

    @Test
    public void test_reader_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);
        StringReader reader = new StringReader("[{ \"key\": 123 },{ \"key2\": 456},{}, true]")
        {
            @Override
            public void close()
            {
                closed.setTrue();
                super.close();
            }
        };

        Mockito.when(arg.eval(Mockito.any()))
                .thenReturn(VectorTestUtils.vv(Column.Type.Any, reader));

        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(arg), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("Value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, Map.of("key", 123), Map.of("key2", 456), Map.of(), true)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(4, rowCount);
        assertTrue(closed.isTrue());
    }

    @Test
    public void test_inputstream_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);
        ByteArrayInputStream baos = new ByteArrayInputStream("""
                [{ "key": 123 },{ "key2": "åäö"},{}, true]

                """.getBytes(StandardCharsets.UTF_8))
        {
            @Override
            public void close() throws IOException
            {
                closed.setTrue();
                super.close();
            }
        };

        Mockito.when(arg.eval(Mockito.any()))
                .thenReturn(VectorTestUtils.vv(Column.Type.Any, baos));

        TupleIterator it = f.execute(context, "", Optional.ofNullable(null), asList(arg), new DatasourceOptions(emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("Value", Column.Type.Any)), List.of(
                            VectorTestUtils.vv(Column.Type.Any, Map.of("key", 123), Map.of("key2", "åäö"), Map.of(), true)
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(4, rowCount);
        assertTrue(closed.isTrue());
    }
}
