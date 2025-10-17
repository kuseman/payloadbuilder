package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link OpenCsvFunction} */
public class OpenCsvFunctionTest extends APhysicalPlanTest
{
    private final TableFunctionInfo f = SystemCatalog.get()
            .getTableFunction("opencsv");

    @Test
    public void test_empty_on_null()
    {
        assertEquals(Arity.ONE, f.arity());
        TupleIterator it = f.execute(context, "", asList(e("null")), new FunctionData(0, emptyList()));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_only_headers()
    {
        TupleIterator it = f.execute(context, "", asList(e("""
                'col1,col2,col3
                '
                """)), new FunctionData(0, emptyList()));

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

        assertEquals(0, rowCount);
    }

    @Test
    public void test_batch_size()
    {
        TupleIterator it = f.execute(context, "", asList(e("""
                'key
                123
                456
                '
                """)), new FunctionData(0, List.of(new Option(IExecutionContext.BATCH_SIZE, intLit(1)))));

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
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.String)), List.of(
                                VectorTestUtils.vv(Column.Type.String, "123")
                                )), next);
                //@formatter:on
            }
            else
            {
                //@formatter:off
                VectorTestUtils.assertTupleVectorsEquals(
                        TupleVector.of(Schema.of(Column.of("key", Column.Type.String)), List.of(
                                VectorTestUtils.vv(Column.Type.String, "456")
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
    public void test_missing_column_value()
    {
        TupleIterator it = f.execute(context, "", asList(e("""
                'key,key2
                123
                1230,4560
                '
                """)), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, null, "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test()
    {
        TupleIterator it = f.execute(context, "", asList(e("""
                'key,key2
                123,1230
                1230,4560
                '
                """)), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_column_separator()
    {
        TupleIterator it = f.execute(context, "", asList(e("""
                'key;key2
                123;1230
                1230;4560
                '
                """)), new FunctionData(0, List.of(new Option(OpenCsvFunction.COLUMN_SEPARATOR, e("';'")))));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_column_separator_headers()
    {
        FunctionData functionData = new FunctionData(0, List.of(new Option(OpenCsvFunction.COLUMN_HEADERS, e("'key;key2'")), new Option(OpenCsvFunction.COLUMN_SEPARATOR, e("';'"))));
        assertEquals(Schema.of(Column.of("key", Type.String), Column.of("key2", Type.String)), f.getSchema(context, "", List.of(), functionData.getOptions()));
        assertEquals(Schema.EMPTY, f.getSchema(context, "", List.of(), emptyList()));

        TupleIterator it = f.execute(context, "", asList(e("""
                '123;1230
                1230;4560
                '
                """)), functionData);

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, "1230", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_reader_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);
        StringReader reader = new StringReader("""
                key,key2
                123,456
                1230,4560
                """)
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

        TupleIterator it = f.execute(context, "", asList(arg), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, "456", "4560")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
        assertTrue(closed.isTrue());
    }

    @Test
    public void test_input_stream_gets_picked_up()
    {
        MutableBoolean closed = new MutableBoolean(false);
        IExpression arg = Mockito.mock(IExpression.class);

        ByteArrayInputStream baos = new ByteArrayInputStream("""
                key,key2
                123,456
                1230,åäö
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

        TupleIterator it = f.execute(context, "", asList(arg), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            assertTrue(it.hasNext());

            TupleVector next = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(Column.of("key", Column.Type.String), Column.of("key2", Column.Type.String)), List.of(
                            VectorTestUtils.vv(Column.Type.String, "123", "1230"),
                            VectorTestUtils.vv(Column.Type.String, "456", "åäö")
                            )), next);
            //@formatter:on

            rowCount += next.getRowCount();
        }
        it.close();

        Assertions.assertThat(f.getDescribeProperties(context, "", asList(arg), new FunctionData(0, emptyList())))
                .containsKey("Bytes Per Second");

        assertEquals(2, rowCount);
        assertTrue(closed.isTrue());
    }
}
