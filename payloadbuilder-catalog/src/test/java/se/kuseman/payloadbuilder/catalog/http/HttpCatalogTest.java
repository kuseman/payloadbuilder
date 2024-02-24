package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.Expectation;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.SeekType;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link HttpCatalog} */
public class HttpCatalogTest
{
    private HttpCatalog catalog = new HttpCatalog();
    private ClientAndServer mockServer;

    @Before
    public void startMockServer()
    {
        mockServer = startClientAndServer();
    }

    @After
    public void stopMockServer()
    {
        mockServer.stop();
    }

    @Test
    public void test_getTableSchema()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        QualifiedName table = QualifiedName.of("http://www.service.com/");
        TableSchema actual;
        // No place holders
        actual = catalog.getTableSchema(context.getSession(), "http", table, emptyList());
        assertEquals(new TableSchema(Schema.EMPTY, emptyList()), actual);

        // Query
        actual = catalog.getTableSchema(context.getSession(), "http", table, List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("id"), ColumnsType.ALL))), actual);

        // Body
        actual = catalog.getTableSchema(context.getSession(), "http", table,
                List.of(new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{ \"keys\": [{{bodyId}}] }"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("bodyId"), ColumnsType.ALL))), actual);

        // Query + body
        actual = catalog.getTableSchema(context.getSession(), "http", table, List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}")),
                new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{ \"keys\": [{{bodyId}}] }"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("id", "bodyId"), ColumnsType.ALL))), actual);
    }

    @Test
    public void test_getSeekDataSource_get()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { 123, 456 }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/123,456")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context, TestUtils.mockOptions(500));
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, 123)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getSeekDataSource_post()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("post")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{\"keys\":[{{id}}]}"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { 123, 456 }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("post")
                .withBody("{\"keys\":[123,456]}"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context, TestUtils.mockOptions(500));
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, 123)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getSeekDataSource_post_strings()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("post")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{\"keys\":[{{id}}]}"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { "value", " value \" with % etc." }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("post")
                .withBody("{\"keys\":[\"value\",\" value \\\" with % etc.\"]}"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context, TestUtils.mockOptions(500));
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, 123)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_invalid_table_name()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        try
        {
            catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http:", "//localhost:" + mockServer.getPort()), data);
        }
        catch (CompileException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Tables qualifiers for HttpCatalog only supportes one part."));
        }
    }

    @Test
    public void test_getScanDataSource_eq_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.eq("id", "id_value"), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, Optional.empty(), predicates, emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        assertEquals("predicate for 'id' column should be consumed", 1, predicates.size());

        Expectation[] mocks = mockServer.when(HttpRequest.request("/id_value")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context, TestUtils.mockOptions(500));
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, 123)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_in_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.in("id", List.of("value1", " value2")), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, Optional.empty(), predicates, emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        assertEquals("predicate for 'id' column should be consumed", 1, predicates.size());

        Expectation[] mocks = mockServer.when(HttpRequest.request("/value1,+value2")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context, TestUtils.mockOptions(500));
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, 123)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_predicate_not_supported_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.notIn("id", List.of("value1", " value2")), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, Optional.empty(), predicates, emptyList(), emptyList(),
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        try
        {
            catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);
        }
        catch (CompileException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("All request placeholders must be used. Placeholders not processed: [id]"));
        }
    }

    @Test
    public void test_query_null_endpoint()
    {
        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            function.execute(context, "http", Optional.empty(), List.of(ExpressionTestUtils.createNullExpression()), options);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            e.printStackTrace();
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("endpoint must be a non null string value"));
        }
    }

    @Test
    public void test_query_function_fallback_response_transformer()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.response("some arbitrary body")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        IDatasourceOptions options = TestUtils.mockOptions(500);
        TupleIterator it = function.execute(context, "http", Optional.empty(), List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())), options);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(FallbackResponseTransformer.SCHEMA, List.of(
                    vv(Type.String, "some arbitrary body"),
                    vv(ResolvedType.table(FallbackResponseTransformer.HEADERS_SCHEMA), TupleVector.of(FallbackResponseTransformer.HEADERS_SCHEMA, List.of(
                            vv(Type.String, "custom-header", "custom-header", "connection", "content-length"),
                            vv(Type.String, "value1", "value2", "keep-alive", "19")
                            )))
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_query_function_non_200()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("error error")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            function.execute(context, "http", Optional.empty(), List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())), options);
            fail("Should fail");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Response was non 200. Code: 404, body: error error"));
        }

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_query_function_non_200_dont_fail()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("error error")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        ILiteralStringExpression failOnNon200 = ExpressionTestUtils.createStringExpression("false");

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        IDatasourceOptions options = Mockito.mock(IDatasourceOptions.class);
        Mockito.when(options.getOptions())
                .thenReturn(List.of(new Option(QualifiedName.of(HttpCatalog.FAIL_ON_NON_200), failOnNon200)));

        TupleIterator it = function.execute(context, "http", Optional.empty(), List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())), options);
        assertFalse(it.hasNext());
        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_query_function_json_response_transformer()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/api")
                .withMethod("post")
                .withHeader("x-header", "custom-value")
                .withBody(body))
                .respond(HttpResponse.response("[{ \"key\": 123 }, { \"key2\": 456 }]")
                        .withHeader("custom-header", "value5", "value5")
                        .withHeader("Content-Type", "application/json"));

        TableFunctionInfo function = catalog.getTableFunction("query");

        ILiteralStringExpression x_header_expression = ExpressionTestUtils.createStringExpression("custom-value");

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        IDatasourceOptions options = Mockito.mock(IDatasourceOptions.class);
        Mockito.when(options.getOption(QualifiedName.of(HttpCatalog.METHOD), context))
                .thenReturn(vv(Type.String, HttpDataSource.POST));
        Mockito.when(options.getOption(QualifiedName.of(QueryFunction.BODY), context))
                .thenReturn(vv(Type.String, body));
        Mockito.when(options.getOptions())
                .thenReturn(List.of(new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), x_header_expression)));

        TupleIterator it = function.execute(context, "http", Optional.empty(), List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort() + "/api")), options);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any), Column.of("key2", Type.Any)), List.of(
                    vv(Type.Any, 123, null),
                    vv(Type.Any, null, 456)
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(2, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("put")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.response("{ \"key\": \"hello world\" }")
                        .withHeader("Content-Type", "application/json"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("put")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        TupleIterator it = dataSource.execute(context, options);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any)), List.of(
                    vv(Type.Any, "hello world")
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        assertEquals(1, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders_non_200_repsonse_code()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("patch")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("some error"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("patch")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            dataSource.execute(context, options);
            fail("Should fail");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Response was non 200. Code: 404, body: some error"));
        }

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders_non_200_repsonse_code_dont_fail()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("patch")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("some error"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("patch")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);
        IDatasourceOptions options = Mockito.mock(IDatasourceOptions.class);
        Mockito.when(options.getOption(QualifiedName.of(HttpCatalog.FAIL_ON_NON_200), context))
                .thenReturn(vv(Type.Boolean, false));

        TupleIterator it = dataSource.execute(context, options);
        assertFalse(it.hasNext());

        mockServer.verify(mocks[0].getId());
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders_missing_endpoint_catalog_property()
    {
        String body = "{\"json\":\"value\"}";
        IExecutionContext context = TestUtils.mockExecutionContext("http", Map.of(), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("put")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "Content-Type"), ExpressionTestUtils.createStringExpression("text/plain"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("endpoint"), data);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            dataSource.execute(context, options);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Missing catalog property 'endpoint' for catalog alias: http"));
        }
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders_unsupported_method()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", Map.of(), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("option"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost"), data);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            dataSource.execute(context, options);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Unsupported request method 'option'"));
        }
    }

    @Test
    public void test_getScanDataSource_with_no_placeholders_unsupported_content_type()
    {
        String body = "{\"json\":\"value\"}";
        IExecutionContext context = TestUtils.mockExecutionContext("http", Map.of("endpoint", "http://localhost"), 0, null);

        DatasourceData data = new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList(),
                asList(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("put")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "Content-Type"), ExpressionTestUtils.createStringExpression("text/plain"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("endpoint"), data);
        IDatasourceOptions options = TestUtils.mockOptions(500);

        try
        {
            dataSource.execute(context, options);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Only application/json Content-Type is supported"));
        }
    }

    private ISeekPredicate mockSeekPrecidate(IExecutionContext context, QualifiedName table, List<String> columns, List<Object[]> values)
    {
        Index index = new Index(table, columns, ColumnsType.ALL);
        ISeekPredicate seekPredicate = mock(ISeekPredicate.class);
        when(seekPredicate.getIndex()).thenReturn(index);
        when(seekPredicate.getIndexColumns()).thenReturn(columns);

        int size = columns.size();
        List<ISeekPredicate.ISeekKey> seekKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ISeekPredicate.ISeekKey seekKey = mock(ISeekPredicate.ISeekKey.class);
            seekKeys.add(seekKey);
            when(seekKey.getType()).thenReturn(SeekType.EQ);
            when(seekKey.getValue()).thenReturn(ValueVector.literalAny(values.get(i)));
        }
        when(seekPredicate.getSeekKeys(any(IExecutionContext.class))).thenReturn(seekKeys);
        return seekPredicate;
    }
}
