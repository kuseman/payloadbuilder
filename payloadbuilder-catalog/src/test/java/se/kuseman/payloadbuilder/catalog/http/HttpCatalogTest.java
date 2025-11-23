package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static se.kuseman.payloadbuilder.test.ExpressionTestUtils.createStringExpression;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.Expectation;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link HttpCatalog} */
class HttpCatalogTest
{
    private HttpCatalog catalog = new HttpCatalog();
    private ClientAndServer mockServer;

    @BeforeEach
    void startMockServer()
    {
        mockServer = startClientAndServer();
    }

    @AfterEach
    void shutdown()
    {
        mockServer.stop();
        catalog.close();
    }

    @Test
    void test_getTableSchema()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        QualifiedName table = QualifiedName.of("http://www.service.com/");
        TableSchema actual;
        // No place holders
        actual = catalog.getTableSchema(context, "http", table, emptyList());
        assertEquals(new TableSchema(Schema.EMPTY, emptyList()), actual);

        // Query
        actual = catalog.getTableSchema(context, "http", table, List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("id"), ColumnsType.ANY))), actual);

        // Body
        actual = catalog.getTableSchema(context, "http", table,
                List.of(new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{ \"keys\": [{{bodyId}}] }"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("bodyId"), ColumnsType.ANY))), actual);

        // Query + body
        actual = catalog.getTableSchema(context, "http", table, List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}")),
                new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{ \"keys\": [{{bodyId}}] }"))));
        assertEquals(new TableSchema(Schema.EMPTY, List.of(new Index(table, asList("id", "bodyId"), ColumnsType.ANY))), actual);
    }

    @Test
    void test_getSeekDataSource_get()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { 123, 456 }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/123%2C456")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getSeekDataSource_post()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, List.of(new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("post")),
                new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{\"keys\":[{{id}}]}"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { 123, 456 }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("post")
                .withBody("{\"keys\":[123,456]}"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getSeekDataSource_post_with_json_response_path()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.METHOD), ExpressionTestUtils.createStringExpression("post")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{\"keys\":[{{key}}]}")),
                        new Option(QualifiedName.of("jsonpath"), ExpressionTestUtils.createStringExpression("/ids"))));

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("key"), List.<Object[]>of(new Object[] { 123, 456 }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("post")
                .withBody("{\"keys\":[123,456]}"))
                .respond(HttpResponse.response("{ \"ids\":[ {\"key\":123} ]}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getSeekDataSource_post_strings()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        //@formatter:off
        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, List.of(
                new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("post")),
                new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression("{\"keys\":[{{id}}]}"))));
        //@formatter:on

        QualifiedName table = QualifiedName.of("http://localhost:" + mockServer.getPort());

        ISeekPredicate seekPrecidate = mockSeekPrecidate(context, table, asList("id"), List.<Object[]>of(new Object[] { "value", " value \" with % etc." }));

        IDatasource dataSource = catalog.getSeekDataSource(context.getSession(), "http", seekPrecidate, data);

        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("post")
                .withBody("{\"keys\":[\"value\",\" value \\\" with % etc.\"]}"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getScanDataSource_invalid_table_name()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        try
        {
            catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http:", "//localhost:" + mockServer.getPort()), data);
        }
        catch (CompileException e)
        {
            assertTrue(e.getMessage()
                    .contains("Tables qualifiers for HttpCatalog only supports one part."), e.getMessage());
        }
    }

    @Test
    void test_getScanDataSource_eq_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.eq("id", "id_value"), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, predicates, emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        assertEquals(1, predicates.size(), "predicate for 'id' column should be consumed");

        Expectation[] mocks = mockServer.when(HttpRequest.request("/id_value")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getScanDataSource_in_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.in("id", List.of("value1", " value2")), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, predicates, emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        assertEquals(1, predicates.size(), "predicate for 'id' column should be consumed");

        Expectation[] mocks = mockServer.when(HttpRequest.request("/value1%2C%2Bvalue2")
                .withMethod("get"))
                .respond(HttpResponse.response("{\"key\":123}")
                        .withHeader("Content-Type", "application/json"));

        TupleIterator it = dataSource.execute(context);
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
    void test_getScanDataSource_predicate_not_supported_predicate()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.notIn("id", List.of("value1", " value2")), IPredicateMock.eq("id2", "value2")));

        DatasourceData data = new DatasourceData(0, predicates, emptyList(), Projection.ALL,
                List.of(new Option(QualifiedName.of(HttpCatalog.QUERY_PATTERN), ExpressionTestUtils.createStringExpression("/{{id}}"))));

        try
        {
            catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);
        }
        catch (CompileException e)
        {
            assertTrue(e.getMessage()
                    .contains("All request placeholders must be used. Placeholders not processed: [id]"), e.getMessage());
        }
    }

    @Test
    void test_query_null_endpoint()
    {
        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        try
        {
            function.execute(context, "http", List.of(ExpressionTestUtils.createNullExpression()), new FunctionData(-1, emptyList()));
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            e.printStackTrace();
            assertTrue(e.getMessage()
                    .contains("endpoint must be a non null string value"), e.getMessage());
        }
    }

    @Test
    void test_query_function_fallback_response_transformer()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.response("some arbitrary body")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        TupleIterator it = function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())), new FunctionData(0, emptyList()));

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
    void test_query_function_non_200()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("error error")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        try
        {
            function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())), new FunctionData(0, emptyList()));
            fail("Should fail");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage()
                    .contains("Response was non 200. Code: 404, body: error error"), e.getMessage());
        }

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_query_function_no_server()
    {
        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        try
        {
            function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:12345")), new FunctionData(0, emptyList()));
            fail("Should fail");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage()
                    .contains("Error performing HTTP request"), e.getMessage());
        }
    }

    @Test
    void test_query_function_no_server_dont_fail()
    {
        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        TupleIterator it = function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:12345")),
                new FunctionData(0, asList(new Option(HttpCatalog.FAIL_ON_NON_200, ExpressionTestUtils.createStringExpression("false")))));
        assertFalse(it.hasNext());
    }

    @Test
    void test_query_function_non_200_dont_fail()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("get"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("error error")
                        .withHeader("custom-header", "value1", "value2"));

        TableFunctionInfo function = catalog.getTableFunction("query");
        assertEquals(Arity.ONE, function.arity());

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        TupleIterator it = function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort())),
                new FunctionData(0, asList(new Option(HttpCatalog.FAIL_ON_NON_200, ExpressionTestUtils.createStringExpression("false")))));
        assertFalse(it.hasNext());
        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_query_function_json_response_transformer()
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

        ILiteralStringExpression x_headerExpression = ExpressionTestUtils.createStringExpression("custom-value");
        ILiteralStringExpression methodExpression = ExpressionTestUtils.createStringExpression("post");
        ILiteralStringExpression bodyExpression = ExpressionTestUtils.createStringExpression(body);

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        //@formatter:off
        TupleIterator it = function.execute(context, "http", List.of(ExpressionTestUtils.createStringExpression("http://localhost:" + mockServer.getPort() + "/api")), new FunctionData(0, List.of(
                new Option(HttpCatalog.METHOD, methodExpression),
                new Option(QueryFunction.BODY, bodyExpression),
                new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), x_headerExpression))
                ));
        //@formatter:on

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
    void test_query_function_json_response_transformer_json_response_path()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/api")
                .withMethod("post")
                .withHeader("x-header", "custom-value")
                .withBody(body))
                .respond(HttpResponse.response("{\"docs\": [{ \"key\": 123 }, { \"key2\": 456 }]}")
                        .withHeader("custom-header", "value5", "value5")
                        .withHeader("Content-Type", "application/json"));

        TableFunctionInfo function = catalog.getTableFunction("query");

        ILiteralStringExpression x_headerExpression = ExpressionTestUtils.createStringExpression("custom-value");
        ILiteralStringExpression methodExpression = ExpressionTestUtils.createStringExpression("post");
        ILiteralStringExpression bodyExpression = ExpressionTestUtils.createStringExpression(body);
        ILiteralStringExpression jsonpathExpression = ExpressionTestUtils.createStringExpression("/docs");

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);
        //@formatter:off
        TupleIterator it = function.execute(context, "http", List.of(createStringExpression("http://localhost:" + mockServer.getPort() + "/api")), new FunctionData(0, List.of(
                new Option(HttpCatalog.METHOD, methodExpression),
                new Option(QualifiedName.of("jsonpath"), jsonpathExpression),
                new Option(QueryFunction.BODY, bodyExpression),
                new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), x_headerExpression))
                ));
        //@formatter:on

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
        it.close();
        assertEquals(2, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_query_function_csv_response_transformer()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/csv")
                .withMethod("get"))
                .respond(HttpResponse.response("""
                        key,key2
                        123,456
                        -120,hello world
                        """)
                        .withHeader("Content-Type", "text/csv"));

        TableFunctionInfo function = catalog.getTableFunction("query");

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        TupleIterator it = function.execute(context, "http", List.of(createStringExpression("http://localhost:" + mockServer.getPort() + "/csv")), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.String), Column.of("key2", Type.String)), List.of(
                    vv(Type.String, "123", "-120"),
                    vv(Type.String, "456", "hello world")
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(2, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_query_function_xml_response_transformer()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/xml")
                .withMethod("get"))
                .respond(HttpResponse.response("""
                        <keys>
                        <key>
                            <key>123</key>
                            <key2>456</key2>
                        </key>
                        <key>
                            <key>-120</key>
                            <key2>hello world</key2>
                        </key>
                        </keys>
                        """)
                        .withHeader("Content-Type", "text/xml"));

        TableFunctionInfo function = catalog.getTableFunction("query");

        IExpression xmlPathExpression = createStringExpression("/keys/key");
        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        TupleIterator it = function.execute(context, "http", List.of(createStringExpression("http://localhost:" + mockServer.getPort() + "/xml")),
                new FunctionData(0, List.of(new Option(QualifiedName.of("xmlpath"), xmlPathExpression))));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.Any), Column.of("key2", Type.Any)), List.of(
                    vv(Type.Any, "123", "-120"),
                    vv(Type.Any, "456", "hello world")
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(2, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_query_function_csv_response_transforme_columnseparator()
    {
        Expectation[] mocks = mockServer.when(HttpRequest.request("/csv")
                .withMethod("get"))
                .respond(HttpResponse.response("""
                        key|key2
                        123|456
                        -120|hello world
                        """)
                        .withHeader("Content-Type", "text/csv"));

        TableFunctionInfo function = catalog.getTableFunction("query");

        IExpression columnSeparatorExpression = createStringExpression("|");

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        TupleIterator it = function.execute(context, "http", List.of(createStringExpression("http://localhost:" + mockServer.getPort() + "/csv")),
                new FunctionData(0, List.of(new Option(QualifiedName.of("columnseparator"), columnSeparatorExpression))));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            //@formatter:off
            VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("key", Type.String), Column.of("key2", Type.String)), List.of(
                    vv(Type.String, "123", "-120"),
                    vv(Type.String, "456", "hello world")
                    ))
                    , v);
            //@formatter:on
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(2, rowCount);

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_getScanDataSource_with_no_placeholders()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("put")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.response("{ \"key\": \"hello world\" }")
                        .withHeader("Content-Type", "application/json"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                asList(new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("put")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        TupleIterator it = dataSource.execute(context);

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
    void test_getScanDataSource_with_no_placeholders_non_200_repsonse_code()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("patch")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("some error"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                asList(new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("patch")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        try
        {
            dataSource.execute(context);
            fail("Should fail");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage()
                    .contains("Response was non 200. Code: 404, body: some error"), e.getMessage());
        }

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_getScanDataSource_with_no_placeholders_non_200_repsonse_code_dont_fail()
    {
        String body = "{\"json\":\"value\"}";
        Expectation[] mocks = mockServer.when(HttpRequest.request("/")
                .withMethod("patch")
                .withBody(body)
                .withHeader("x-header", "x-value"))
                .respond(HttpResponse.notFoundResponse()
                        .withBody("some error"));

        IExecutionContext context = TestUtils.mockExecutionContext("http", emptyMap(), 0, null);

        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                asList(new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("patch")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "x-header"), ExpressionTestUtils.createStringExpression("x-value")),
                        new Option(HttpCatalog.FAIL_ON_NON_200, ExpressionTestUtils.createStringExpression("false"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("http://localhost:" + mockServer.getPort()), data);

        TupleIterator it = dataSource.execute(context);
        assertFalse(it.hasNext());

        mockServer.verify(mocks[0].getId());
    }

    @Test
    void test_getScanDataSource_with_no_placeholders_missing_endpoint_catalog_property()
    {
        String body = "{\"json\":\"value\"}";
        IExecutionContext context = TestUtils.mockExecutionContext("http", Map.of(), 0, null);

        DatasourceData data = new DatasourceData(0, emptyList(), emptyList(), Projection.ALL,
                asList(new Option(HttpCatalog.METHOD, ExpressionTestUtils.createStringExpression("put")),
                        new Option(QualifiedName.of(HttpCatalog.BODY_PATTERN), ExpressionTestUtils.createStringExpression(body)),
                        new Option(QualifiedName.of(HttpCatalog.HEADER, "Content-Type"), ExpressionTestUtils.createStringExpression("text/plain"))));
        IDatasource dataSource = catalog.getScanDataSource(context.getSession(), "http", QualifiedName.of("endpoint"), data);

        try
        {
            dataSource.execute(context);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Missing catalog property 'endpoint' for catalog alias: http"), e.getMessage());
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
            when(seekKey.getValue()).thenReturn(VectorTestUtils.vv(Type.Any, values.get(i)));
        }
        when(seekPredicate.getSeekKeys(any(IExecutionContext.class))).thenReturn(seekKeys);
        return seekPredicate;
    }
}
