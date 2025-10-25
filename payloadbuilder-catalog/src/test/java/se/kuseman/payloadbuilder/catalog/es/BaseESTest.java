package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.ThreadUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Base class for elastic test container tests */
abstract class BaseESTest extends Assert
{
    private static final String CATALOG_ALIAS = "es";
    private static final String INDEX = "test";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String endpoint;
    private final String type;
    private final Version version;
    private final ESCatalog catalog = new ESCatalog();

    //@formatter:off
    private List<Pair<String, Map<String, Object>>> testData = asList(
        Pair.of("001", ofEntries(
            entry("key", 123)
        )),
        Pair.of("002", ofEntries(
            entry("key", 456)
        )),
        Pair.of("003", ofEntries(
            entry("key", 789),
            entry("key2", "some string")
        ))
    );
    //@formatter:on

    BaseESTest(String endpoint, String type, Version version)
    {
        this.endpoint = endpoint;
        this.type = type;
        this.version = version;
    }

    @Before
    public void setup() throws IOException
    {
        HttpGet mappings = new HttpGet(endpoint + "/_mappings");
        // Set<String> indices = emptySet();

        Set<String> indices = HttpClientUtils.execute("", mappings, null, null, null, null, null, null, response ->
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = MAPPER.readValue(response.getEntity()
                    .getContent(), Map.class);
            return m.keySet();
        });

        System.out.println(getClass().getSimpleName() + " deleting indices: " + indices);

        for (String index : indices)
        {
            if (index.startsWith("."))
            {
                continue;
            }

            HttpDelete delete = new HttpDelete(endpoint + "/" + index);

            HttpClientUtils.execute("", delete, null, null, null, null, null, null, response ->
            {
                if (!(response.getCode() >= 200
                        && response.getCode() < 299))
                {
                    String error = IOUtils.toString(response.getEntity()
                            .getContent(), StandardCharsets.UTF_8);
                    throw new RuntimeException("Error index document: " + error);
                }
                return null;
            });
        }
    }

    @Test
    public void test_multi_type_scan() throws IOException
    {
        assumeTrue(version.getStrategy()
                .supportsTypes());

        createIndex(endpoint, INDEX);

        index(endpoint, INDEX, "type1", MAPPER.writeValueAsString(ofEntries(entry("key", 123))), "001");
        index(endpoint, INDEX, "type2", MAPPER.writeValueAsString(ofEntries(entry("key", 456))), "002");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("_doc"), new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify that sort items are consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
            assertVectorsEquals(vv(Type.Any, "type1", "type2"), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "001", "002"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 2);
            assertVectorsEquals(vv(Type.Any, 123, 456), v.getColumn(4));
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(2, rowCount);
    }

    @Test
    public void test_system_tables() throws IOException
    {
        createIndex(endpoint, INDEX);
        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("tables"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("name", Type.String),
                    Column.of("properties", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            if (version.getStrategy()
                    .supportsTypes())
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, type), v.getColumn(0));
                assertVectorIsMap(v.getColumn(1), 2);
            }
            else
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME), v.getColumn(0));
                assertVectorIsMap(v.getColumn(1), 1);
            }
        }
        assertEquals(version.getStrategy()
                .supportsTypes() ? 2
                        : 1,
                rowCount);
    }

    @Test
    public void test_system_indices() throws IOException
    {
        createIndex(endpoint, INDEX);
        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("indices"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            assertEquals(ESCatalog.INDICES_SCHEMA.getSchema(), v.getSchema());

            // Special case for 1.x and 2.x since those doesn't auto create
            // fields to string values that is not analyzed and hence
            // is not supported for indices (key2)
            if (version == Version._1X
                    || version == Version._2X)
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME, type, type), v.getColumn(0));
                assertVectorsEquals(vv(Type.Any, asList("__id"), asList("key"), asList("__id"), asList("key")), v.getColumn(1));
            }
            else if (version.getStrategy()
                    .supportsTypes())
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME, type, type, type), v.getColumn(0));
                assertVectorsEquals(vv(Type.Any, asList("__id"), asList("key2"), asList("key"), asList("__id"), asList("key2"), asList("key")), v.getColumn(1));
            }
            else
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME), v.getColumn(0));
                assertVectorsEquals(vv(Type.Any, asList("__id"), asList("key2"), asList("key")), v.getColumn(1));
            }
        }

        if (version == Version._1X
                || version == Version._2X)
        {
            assertEquals(4, rowCount);
        }
        else if (version.getStrategy()
                .supportsTypes())
        {
            assertEquals(6, rowCount);
        }
        else
        {
            assertEquals(3, rowCount);
        }
    }

    @Test
    public void test_system_columns() throws IOException
    {
        createIndex(endpoint, INDEX);
        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("columns"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            assertEquals("table", v.getSchema()
                    .getColumns()
                    .get(0)
                    .getName());
            assertEquals("name", v.getSchema()
                    .getColumns()
                    .get(1)
                    .getName());

            if (version.getStrategy()
                    .supportsTypes())
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME, type, type), v.getColumn(0));
                assertVectorsEquals(vv(Type.String, "key", "key2", "key", "key2"), v.getColumn(1));
            }
            else
            {
                assertVectorsEquals(vv(Type.String, ESCatalog.SINGLE_TYPE_TABLE_NAME, ESCatalog.SINGLE_TYPE_TABLE_NAME), v.getColumn(0));
                assertVectorsEquals(vv(Type.String, "key", "key2"), v.getColumn(1));
            }
        }
        assertEquals(version.getStrategy()
                .supportsTypes() ? 4
                        : 2,
                rowCount);
    }

    @Test
    public void test_search_body() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        SearchFunction f = new SearchFunction();

        INamedExpression body = mock(INamedExpression.class);
        when(body.getName()).thenReturn("body");
        when(body.eval(any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\" }", 1));
        when(body.eval(any(), any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\" }", 1));

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        TupleIterator it = f.execute(context, CATALOG_ALIAS, asList(body), new FunctionData(0, emptyList()));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();
            assertScan(v);
        }
        assertEquals(3, rowCount);
    }

    @Test
    public void test_search_body_with_scroll_size_in_options() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        SearchFunction f = new SearchFunction();

        INamedExpression body = mock(INamedExpression.class);
        when(body.getName()).thenReturn("body");
        when(body.eval(any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\" }", 1));
        when(body.eval(any(), any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\" }", 1));
        INamedExpression scroll = mock(INamedExpression.class);
        when(scroll.getName()).thenReturn("scroll");
        when(scroll.eval(any())).thenReturn(ValueVector.literalBoolean(true, 1));
        when(scroll.eval(any(), any())).thenReturn(ValueVector.literalBoolean(true, 1));

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        TupleIterator it = f.execute(context, CATALOG_ALIAS, asList(body, scroll),
                new FunctionData(0, List.of(new Option(IExecutionContext.BATCH_SIZE, ExpressionTestUtils.createIntegerExpression(2)))));

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            if (batchCount == 0)
            {
                // First batch should not contain key2
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "001", "002"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 2);
                assertVectorsEquals(vv(Type.Any, 123, 456), v.getColumn(4));
            }
            else
            {
                // Second batch have both key and key2
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any),
                        Column.of("key2", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "003"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 789), v.getColumn(4));
                assertVectorsEquals(vv(Type.Any, "some string"), v.getColumn(5));
            }
            batchCount++;
            rowCount += v.getRowCount();
        }
        assertEquals(2, batchCount);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_search_body_with_scroll_size_in_body() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        SearchFunction f = new SearchFunction();

        INamedExpression body = mock(INamedExpression.class);
        when(body.getName()).thenReturn("body");
        when(body.eval(any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\", \"size\": 2 }", 1));
        when(body.eval(any(), any())).thenReturn(ValueVector.literalString("{ \"sort\": \"key\", \"size\": 2 }", 1));
        INamedExpression scroll = mock(INamedExpression.class);
        when(scroll.getName()).thenReturn("scroll");
        when(scroll.eval(any())).thenReturn(ValueVector.literalBoolean(true, 1));
        when(scroll.eval(any(), any())).thenReturn(ValueVector.literalBoolean(true, 1));

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, new ESDatasource.Data());

        TupleIterator it = f.execute(context, CATALOG_ALIAS, asList(body, scroll), new FunctionData(0, emptyList()));

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            if (batchCount == 0)
            {
                // First batch should not contain key2
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "001", "002"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 2);
                assertVectorsEquals(vv(Type.Any, 123, 456), v.getColumn(4));
            }
            else
            {
                // Second batch have both key and key2
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any),
                        Column.of("key2", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "003"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 789), v.getColumn(4));
                assertVectorsEquals(vv(Type.Any, "some string"), v.getColumn(5));
            }

            batchCount++;
            rowCount += v.getRowCount();
        }
        assertEquals(2, batchCount);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_cat()
    {
        CatFunction f = new CatFunction();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", "")), 0, new ESDatasource.Data());
        IExpression catSpecArg = mock(IExpression.class);
        when(catSpecArg.eval(context)).thenReturn(VectorTestUtils.vv(Type.String, "/nodes"));

        TupleIterator it = f.execute(context, CATALOG_ALIAS, asList(catSpecArg), new FunctionData(0, emptyList()));
        int count = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(1, v.getRowCount());
            // Verify that we have columns, each ES version return quite different and non deterministic node data
            assertTrue(v.getSchema()
                    .getSize() > 0);
            count++;
        }

        assertEquals(1, count);
    }

    @Test
    public void test_nested() throws IOException
    {
        //@formatter:off
        Map<String, Object> mappings = ofEntries(
            entry("properties", ofEntries(
                entry("key", ofEntries(
                    entry("type", "nested"),
                    entry("properties", ofEntries(
                        entry("value", ofEntries(
                            entry("type", "integer")
                        )),
                        entry("sum", ofEntries(
                            entry("type", "integer")
                        ))
                    ))
                ))
            ))
        );
        //@formatter:on

        mappings = modifyMappingProperties(mappings);

        createIndex(endpoint, INDEX, ofEntries(entry("mappings", mappings)));

        //@formatter:off
        List<Pair<String, Map<String, Object>>> testDocs = asList(
            Pair.of("001", ofEntries(
                entry("key", asList(
                    ofEntries(entry("value", 1), entry("sum", 10)),
                    ofEntries(entry("value", 2), entry("sum", 20)),
                    ofEntries(entry("value", 3), entry("sum", 30))
                ))
            )),
            Pair.of("003", ofEntries(
                entry("key", asList(
                    ofEntries(entry("value", 7), entry("sum", 70)),
                    ofEntries(entry("value", 8), entry("sum", 80)),
                    ofEntries(entry("value", 9), entry("sum", 90))
                ))
            )),
            Pair.of("002", ofEntries(
                entry("key", asList(
                    ofEntries(entry("value", 4), entry("sum", 40)),
                    ofEntries(entry("value", 5), entry("sum", 50)),
                    ofEntries(entry("value", 6), entry("sum", 60))
                ))
            )),
            Pair.of("005", ofEntries(
                entry("key", asList(
                    ofEntries(entry("value", 10), entry("sum", 100)),
                    ofEntries(entry("value", 11), entry("sum", 110)),
                    ofEntries(entry("value", 12), entry("sum", 120))
                ))
            )),
            Pair.of("004", ofEntries(
                entry("key", asList(
                    ofEntries(entry("value", 13), entry("sum", 130)),
                    ofEntries(entry("value", 14), entry("sum", 140)),
                    ofEntries(entry("value", 15), entry("sum", 150))
                ))
            ))
        );
        //@formatter:on
        for (Pair<String, Map<String, Object>> p : testDocs)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        // Test single qualifier with mixed case gets splitted and works
        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key.VALUE"), Order.DESC)));
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.comparison(QualifiedName.of("key", "SUM"), 100, IComparisonExpression.Type.LESS_THAN)));

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(version.getStrategy()
                .supportsTypes() ? type
                        : ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, predicates, sortItems, Projection.ALL, emptyList()));

        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();
            assertVectorsEquals(vv(Type.Any, "003", "002", "001"), v.getColumn(2));
        }
        it.close();

        assertEquals(data.requestCount, 3);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_index_non_id_field() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);
        // Test non matching case of index field
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, "KEY", 123, null); // Null values should be excluded
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            rowCount += v.getRowCount();
            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "001"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 1);
            assertVectorsEquals(vv(Type.Any, 123), v.getColumn(4));
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(1, rowCount);
    }

    @Test
    public void test_datasource_index_id_field() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, "__id", "001", null); // Null values should be excluded
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            rowCount += v.getRowCount();
            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "001"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 1);
            assertVectorsEquals(vv(Type.Any, 123), v.getColumn(4));
        }
        it.close();

        assertEquals(data.requestCount, 1);
        assertEquals(1, rowCount);
    }

    @Test
    public void test_datasource_index_non_id_field_batching() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, "key", 123, 456);
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, List.of(
                // Size 1 => 2 batches
                new Option(IExecutionContext.BATCH_SIZE, ExpressionTestUtils.createIntegerExpression(1)))));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        int batchCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            batchCount++;

            rowCount += v.getRowCount();
            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            if (batchCount == 1)
            {
                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "001"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 123), v.getColumn(4));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "002"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 456), v.getColumn(4));
            }
        }
        it.close();

        // 3 scrolls 1 close
        assertEquals(4, data.requestCount);
        assertEquals(2, rowCount);
        assertEquals(2, batchCount);
    }

    @Test
    public void test_datasource_sort_on_index() throws IOException
    {
        createIndex(endpoint, "test_001");
        createIndex(endpoint, "test_002");
        createIndex(endpoint, "test_003");

        index(endpoint, "test_001", type, "{}", "001");
        index(endpoint, "test_002", type, "{}", "002");
        index(endpoint, "test_003", type, "{}", "003");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", "test*")), 0, data);
        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("__index"), Order.DESC)));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify that sort items are consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            rowCount += v.getRowCount();
            assertVectorsEquals(vv(Type.Any, "test_003", "test_002", "test_001"), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(3), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "003", "002", "001"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 3);
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_filter_on_index() throws IOException
    {
        // Somehow index is not filter:able in 1.x
        assumeTrue(version != Version._1X);

        createIndex(endpoint, "test_001");
        createIndex(endpoint, "test_002");
        createIndex(endpoint, "test_003");

        index(endpoint, "test_001", type, "{}", "001");
        index(endpoint, "test_002", type, "{}", "002");
        index(endpoint, "test_003", type, "{}", "003");

        ESDatasource.Data data = new ESDatasource.Data();
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.eq("__index", "test_002")));

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", "*")), 0, data);
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, predicates, emptyList(), Projection.ALL, emptyList()));

        // Verify that predicates are consumed
        assertTrue(predicates.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            rowCount += v.getRowCount();
            assertVectorsEquals(vv(Type.Any, "test_002"), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "002"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 1);
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(1, rowCount);
    }

    @Test
    public void test_datasource_filter_on_type() throws IOException
    {
        assumeTrue(version.getStrategy()
                .supportsTypes());

        createIndex(endpoint, INDEX);

        index(endpoint, INDEX, "type1", MAPPER.writeValueAsString(ofEntries(entry("key", 123))), "001");
        index(endpoint, INDEX, "type2", MAPPER.writeValueAsString(ofEntries(entry("key", 456))), "002");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.eq("__type", "type2")));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("_doc"), new DatasourceData(0, predicates, emptyList(), Projection.ALL, emptyList()));

        // Verify that predicates are consumed
        assertTrue(predicates.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
            assertVectorsEquals(vv(Type.Any, "type2"), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "002"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 1);
            assertVectorsEquals(vv(Type.Any, 456), v.getColumn(4));
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(1, rowCount);
    }

    @Test
    public void test_datasource_filter_on_id() throws IOException
    {
        createIndex(endpoint, INDEX);

        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 123))), "001");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 456))), "002");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.eq("__id", "002")));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("_doc"), new DatasourceData(0, predicates, emptyList(), Projection.ALL, emptyList()));

        // Verify that predicates are consumed
        assertTrue(predicates.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "002"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 1);
            assertVectorsEquals(vv(Type.Any, 456), v.getColumn(4));
        }
        it.close();

        assertEquals(1, data.requestCount);
        assertEquals(1, rowCount);
    }

    @Test
    public void test_datasource_filter_on_id_with_in() throws IOException
    {
        createIndex(endpoint, INDEX);

        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 123))), "001");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 456))), "002");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 789))), "003");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 101112))), "004");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key"))));
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.in("__id", asList("002", "004"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("_doc"), new DatasourceData(0, predicates, sortItems, Projection.ALL, emptyList()));

        // Verify that predicates/sort items are consumed
        assertTrue(predicates.isEmpty());
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "002", "004"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 2);
            assertVectorsEquals(vv(Type.Any, 456, 101112), v.getColumn(4));
        }
        it.close();

        assertEquals(1, data.requestCount);
        assertEquals(2, rowCount);
    }

    @Test
    public void test_datasource_filter_on_id_with_not_in() throws IOException
    {
        createIndex(endpoint, INDEX);

        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 123))), "001");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 456))), "002");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 789))), "003");
        index(endpoint, INDEX, type, MAPPER.writeValueAsString(ofEntries(entry("key", 101112))), "004");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key"))));
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.notIn("__id", asList("001", "003"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("_doc"), new DatasourceData(0, predicates, sortItems, Projection.ALL, emptyList()));

        // Verify that predicates/sort items are consumed
        assertTrue(predicates.isEmpty());
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            rowCount += v.getRowCount();

            //@formatter:off
            assertEquals(Schema.of(
                    Column.of("__index", Type.Any),
                    Column.of("__type", Type.Any),
                    Column.of("__id", Type.Any),
                    Column.of("__meta", Type.Any),
                    Column.of("key", Type.Any)
                    ), v.getSchema());
            //@formatter:on

            assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
            assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
            assertVectorsEquals(vv(Type.Any, "002", "004"), v.getColumn(2));
            assertVectorIsMap(v.getColumn(3), 2);
            assertVectorsEquals(vv(Type.Any, 456, 101112), v.getColumn(4));
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(2, rowCount);
    }

    @Test
    public void test_datasource_index_id_field_batching() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, "__id", "001", "002", "003");
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, List.of(
                // Size 2 => 2 batches
                new Option(IExecutionContext.BATCH_SIZE, ExpressionTestUtils.createIntegerExpression(2)))));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        int batchCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            batchCount++;

            rowCount += v.getRowCount();

            if (batchCount == 1)
            {
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "001", "002"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 2);
                assertVectorsEquals(vv(Type.Any, 123, 456), v.getColumn(4));
            }
            else
            {
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any),
                        Column.of("key2", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "003"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 789), v.getColumn(4));
                assertVectorsEquals(vv(Type.Any, "some string"), v.getColumn(5));
            }
        }
        it.close();

        assertEquals(2, data.requestCount);
        assertEquals(3, rowCount);
        assertEquals(2, batchCount);
    }

    @Test
    public void test_datasource_table_scan() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(version.getStrategy()
                .supportsTypes() ? type
                        : ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify that sort items are consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            rowCount += v.getRowCount();

            assertScan(v);
        }
        it.close();

        assertEquals(3, data.requestCount);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_table_scan_batching() throws IOException
    {
        createIndex(endpoint, INDEX);

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, INDEX, type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, ofEntries(entry("endpoint", endpoint), entry("index", INDEX)), 0, data);

        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("key"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(version.getStrategy()
                .supportsTypes() ? type
                        : ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, emptyList(), sortItems, Projection.ALL, List.of(
                        // Size 2 => 2 batches
                        new Option(IExecutionContext.BATCH_SIZE, ExpressionTestUtils.createIntegerExpression(2)))));

        // Verify that sort items are consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        int batchCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            batchCount++;
            rowCount += v.getRowCount();

            if (batchCount == 1)
            {
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(2), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "001", "002"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 2);
                assertVectorsEquals(vv(Type.Any, 123, 456), v.getColumn(4));
            }
            else
            {
                //@formatter:off
                assertEquals(Schema.of(
                        Column.of("__index", Type.Any),
                        Column.of("__type", Type.Any),
                        Column.of("__id", Type.Any),
                        Column.of("__meta", Type.Any),
                        Column.of("key", Type.Any),
                        Column.of("key2", Type.Any)
                        ), v.getSchema());
                //@formatter:on

                assertVectorsEquals(vv(Type.Any, INDEX), v.getColumn(0));
                assertVectorsEquals(getTypeColumnExpected(1), v.getColumn(1));
                assertVectorsEquals(vv(Type.Any, "003"), v.getColumn(2));
                assertVectorIsMap(v.getColumn(3), 1);
                assertVectorsEquals(vv(Type.Any, 789), v.getColumn(4));
                assertVectorsEquals(vv(Type.Any, "some string"), v.getColumn(5));
            }
        }
        it.close();

        assertEquals(4, data.requestCount);
        assertEquals(2, batchCount);
        assertEquals(3, rowCount);
    }

    @Test
    public void test_ingore_unavailable_indices() throws IOException, InterruptedException
    {
        createIndex(endpoint, "ignoretest_opened");
        createIndex(endpoint, "ignoretest_closed");

        for (Pair<String, Map<String, Object>> p : testData)
        {
            index(endpoint, "ignoretest_opened", type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
            index(endpoint, "ignoretest_closed", type, MAPPER.writeValueAsString(p.getValue()), p.getKey());
        }

        // Let indices be created before closing
        ThreadUtils.sleep(Duration.ofMillis(250));

        closeIndex(endpoint, "ignoretest_closed");

        ESDatasource.Data data = new ESDatasource.Data();
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, Map.of("endpoint", endpoint, "index", "ignoretest_opened,ignoretest_closed"), 0, data);
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(version.getStrategy()
                .supportsTypes() ? type
                        : ESCatalog.SINGLE_TYPE_TABLE_NAME),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertVectorsEquals(vv(Type.Any, "ignoretest_opened", "ignoretest_opened", "ignoretest_opened"), next.getColumn(0));

            rowCount += next.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    protected Map<String, Object> modifyMappingProperties(Map<String, Object> mappings)
    {
        // Wrap the mapping with a type
        return ofEntries(entry(type, mappings));
    }

    private void assertScan(TupleVector v)
    {
        //@formatter:off
        assertEquals(Schema.of(
                Column.of("__index", Type.Any),
                Column.of("__type", Type.Any),
                Column.of("__id", Type.Any),
                Column.of("__meta", Type.Any),
                Column.of("key", Type.Any),
                Column.of("key2", Type.Any)
                ), v.getSchema());
        //@formatter:on

        assertVectorsEquals(vv(Type.Any, INDEX, INDEX, INDEX), v.getColumn(0));
        assertVectorsEquals(getTypeColumnExpected(3), v.getColumn(1));
        assertVectorsEquals(vv(Type.Any, "001", "002", "003"), v.getColumn(2));
        assertVectorIsMap(v.getColumn(3), 3);
        assertVectorsEquals(vv(Type.Any, 123, 456, 789), v.getColumn(4));
        assertVectorsEquals(vv(Type.Any, null, null, "some string"), v.getColumn(5));
    }

    private void assertVectorIsMap(ValueVector v, int expectedSize)
    {
        int size = v.size();
        assertEquals(expectedSize, size);
        for (int i = 0; i < size; i++)
        {
            Object val = v.valueAsObject(i);
            if (!(val == null
                    || val instanceof Map))
            {
                fail("Vector values should be null or maps");
            }
        }
    }

    protected ValueVector getTypeColumnExpected(int size)
    {
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public Object getAny(int row)
            {
                return type;
            }
        };
    }

    protected void closeIndex(String endpoint, String index) throws IOException
    {
        HttpPost post = new HttpPost(endpoint + "/" + index + "/_close");
        HttpClientUtils.execute("", post, null, null, null, null, null, null, response ->
        {
            if (!(response.getCode() >= 200
                    && response.getCode() < 299))
            {
                String error = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                throw new RuntimeException("Error closing index: " + error);
            }
            return null;
        });
    }

    protected void createIndex(String endpoint, String index) throws IOException
    {
        createIndex(endpoint, index, null);
    }

    protected void createIndex(String endpoint, String index, Map<String, Object> body) throws IOException
    {
        HttpPut put = new HttpPut(endpoint + "/" + index);
        if (body != null)
        {
            put.setEntity(new StringEntity(MAPPER.writeValueAsString(body), StandardCharsets.UTF_8));
        }

        HttpClientUtils.execute("", put, null, null, null, null, null, null, response ->
        {
            if (!(response.getCode() >= 200
                    && response.getCode() < 299))
            {
                String error = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                throw new RuntimeException("Error creating index: " + error);
            }
            return null;
        });
    }

    protected void index(String endpoint, String index, String type, String source, String id) throws IOException
    {
        ClassicHttpRequest req;
        if (id == null)
        {
            HttpPost post = new HttpPost(endpoint + "/" + index + "/" + type + "?refresh=true");
            post.setEntity(new StringEntity(source, StandardCharsets.UTF_8));
            req = post;
        }
        else
        {
            req = createIndexRequest(endpoint, index, type, id, source);
        }

        HttpClientUtils.execute("", req, null, null, null, null, null, null, response ->
        {
            if (!(response.getCode() >= 200
                    && response.getCode() < 299))
            {
                String error = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                throw new RuntimeException("Error index document: " + error);
            }
            return null;
        });
    }

    /** Return index url */
    protected ClassicHttpRequest createIndexRequest(String endpoint, String index, String type, String id, String source)
    {
        HttpPut put = new HttpPut(endpoint + "/" + index + "/" + type + "/" + id + "?refresh=true");
        put.setEntity(new StringEntity(source, StandardCharsets.UTF_8));
        return put;
    }

    private ISeekPredicate mockSeekPrecidate(IExecutionContext context, String column, Object... values)
    {
        // Fetch indices and extract the one on key field
        QualifiedName table = QualifiedName.of(version.getStrategy()
                .supportsTypes() ? type
                        : ESCatalog.SINGLE_TYPE_TABLE_NAME);
        TableSchema tableSchema = catalog.getTableSchema(context.getSession(), CATALOG_ALIAS, table, emptyList());
        Index index = tableSchema.getIndices()
                .stream()
                .filter(i -> i.getColumns()
                        .stream()
                        .anyMatch(c -> c.equalsIgnoreCase(column)))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Index on key should exist"));

        ISeekPredicate seekPredicate = mock(ISeekPredicate.class);
        when(seekPredicate.getIndex()).thenReturn(index);
        when(seekPredicate.getIndexColumns()).thenReturn(asList(column));
        ISeekPredicate.ISeekKey seekKey = mock(ISeekPredicate.ISeekKey.class);
        when(seekPredicate.getSeekKeys(any(IExecutionContext.class))).thenReturn(asList(seekKey));
        when(seekKey.getValue()).thenReturn(VectorTestUtils.vv(Type.Any, values));

        return seekPredicate;
    }
}
