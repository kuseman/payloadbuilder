package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Base class for MongoDB test container tests. */
abstract class BaseMongoTest
{
    private static final String CATALOG_ALIAS = "mongo";
    private static final String DATABASE = "testdb";
    private static final String COLLECTION = "testcoll";
    static final ObjectId ID_1 = new ObjectId("507f1f77bcf86cd799439011");
    static final ObjectId ID_2 = new ObjectId("507f1f77bcf86cd799439012");
    static final ObjectId ID_3 = new ObjectId("507f1f77bcf86cd799439013");

    private final String connectionString;
    private final MongoCatalog catalog = new MongoCatalog();
    private MongoClient testClient;

    BaseMongoTest(String connectionString)
    {
        this.connectionString = connectionString;
    }

    @BeforeEach
    void setup()
    {
        testClient = MongoClients.create(connectionString);
        testClient.getDatabase(DATABASE)
                .getCollection(COLLECTION)
                .drop();
    }

    @AfterEach
    void teardown()
    {
        testClient.close();
        catalog.close();
    }

    private MongoCollection<Document> collection()
    {
        return testClient.getDatabase(DATABASE)
                .getCollection(COLLECTION);
    }

    private void insertTestData()
    {
        collection().insertMany(asList(new Document("_id", ID_1).append("key", 123)
                .append("name", "one"),
                new Document("_id", ID_2).append("key", 456)
                        .append("name", "two"),
                new Document("_id", ID_3).append("key", 789)
                        .append("name", "three")));
        collection().createIndex(Indexes.ascending("key"));
    }

    private IExecutionContext mockContext()
    {
        return mockContext(IExecutionContext.DEFAULT_BATCH_SIZE);
    }

    private IExecutionContext mockContext(int batchSize)
    {
        // NOTE! MongoDatasource.execute() always populates a MongoDatasource.Data via getOrCreateNodeData - pass a real instance here (not null)
        // otherwise the mocked getOrCreateNodeData(...) stub returns null and execute() NPEs.
        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, Map.of(MongoCatalog.CONNECTIONSTRING_KEY, connectionString), 0, new MongoDatasource.Data());
        // NOTE! getBatchSize is a default method on IExecutionContext - stub it directly rather than relying on the mocked context falling through to
        // the real default implementation (that fallthrough is broken for interface default methods under some JDK/Mockito combinations).
        doReturn(batchSize).when(context)
                .getBatchSize(any());
        return context;
    }

    @Test
    void test_scan()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, new ArrayList<>(), new ArrayList<>(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        assertEquals(3, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
        assertEquals(Set.of(123, 456, 789), collectColumnValues(batches, "key"));
    }

    @Test
    void test_describe_shows_predicate_and_query_before_and_after_execution()
    {
        insertTestData();
        IExecutionContext context = mockContext();
        IPredicate predicate = IPredicateMock.eq("key", 456);
        when(predicate.getSqlRepresentation()).thenReturn("key = 456");
        List<IPredicate> predicates = new ArrayList<>(List.of(predicate));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, predicates, new ArrayList<>(), Projection.ALL, emptyList()));

        Map<String, Object> before = ds.getDescribeProperties(context);
        assertEquals(MongoCatalog.NAME, before.get(IDatasource.CATALOG));
        assertTrue(((String) before.get(IDatasource.PREDICATE)).contains("key"));
        assertTrue(((String) before.get("Query")).contains("456"));
        assertNull(before.get("Document count"));

        drain(ds.execute(context));

        // Wire getNodeData to return the same Data instance execute() just populated via getOrCreateNodeData
        MongoDatasource.Data nodeData = context.getStatementContext()
                .getOrCreateNodeData(0, MongoDatasource.Data::new);
        when(context.getStatementContext().<MongoDatasource
                .Data>getNodeData(0)).thenReturn(nodeData);

        Map<String, Object> after = ds.getDescribeProperties(context);
        assertEquals(1L, after.get("Document count"));
        assertEquals(1, after.get("Request count"));
        assertTrue(((String) after.get("Query")).contains("456"));
    }

    @Test
    void test_scan_respects_batch_size()
    {
        insertTestData();
        IExecutionContext context = mockContext(1);

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, new ArrayList<>(), new ArrayList<>(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        assertEquals(3, batches.size());
        assertEquals(3, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
    }

    @Test
    void test_seek_by_id()
    {
        insertTestData();
        IExecutionContext context = mockContext();
        ISeekPredicate seekPredicate = mockSeekPredicate(context, "_id", ID_2.toHexString());

        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, new ArrayList<>(), new ArrayList<>(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        assertEquals(1, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
        assertEquals(Set.of(456), collectColumnValues(batches, "key"));
    }

    @Test
    void test_seek_by_secondary_index()
    {
        insertTestData();
        IExecutionContext context = mockContext();
        ISeekPredicate seekPredicate = mockSeekPredicate(context, "key", 456);

        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, new ArrayList<>(), new ArrayList<>(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        assertEquals(1, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
        assertEquals(Set.of(456), collectColumnValues(batches, "key"));
    }

    @Test
    void test_predicate_pushdown_is_consumed_and_filters_rows()
    {
        insertTestData();
        IExecutionContext context = mockContext();
        List<IPredicate> predicates = new ArrayList<>(List.of(IPredicateMock.gt("key", 123)));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, predicates, new ArrayList<>(), Projection.ALL, emptyList()));

        assertTrue(predicates.isEmpty());

        List<TupleVector> batches = drain(ds.execute(context));
        assertEquals(Set.of(456, 789), collectColumnValues(batches, "key"));
    }

    @Test
    void test_sort_pushdown_is_consumed_and_orders_rows()
    {
        insertTestData();
        IExecutionContext context = mockContext();
        List<ISortItem> sortItems = new ArrayList<>(List.of(se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem(QualifiedName.of("key"), ISortItem.Order.DESC)));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList()));

        assertTrue(sortItems.isEmpty());

        List<TupleVector> batches = drain(ds.execute(context));
        List<Object> keys = new ArrayList<>();
        for (TupleVector v : batches)
        {
            ValueVector vv = v.getColumn(indexOf(v, "key"));
            for (int i = 0; i < v.getRowCount(); i++)
            {
                keys.add(vv.getAny(i));
            }
        }
        assertEquals(List.of(789, 456, 123), keys);
    }

    @Test
    void test_projection_pushdown_limits_columns()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(DATABASE, COLLECTION),
                new DatasourceData(0, new ArrayList<>(), new ArrayList<>(), Projection.columns(List.of("key")), emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        for (TupleVector v : batches)
        {
            assertEquals(List.of("key"), v.getSchema()
                    .getColumns()
                    .stream()
                    .map(Column::getName)
                    .collect(toList()));
        }
    }

    @Test
    void test_aggregate_function()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        AggregateFunction function = (AggregateFunction) catalog.getTableFunction("aggregate");
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression(DATABASE + "." + COLLECTION), ExpressionTestUtils.createStringExpression("[{\"$match\": {\"key\": 456}}]"));

        TupleIterator it = function.execute(context, CATALOG_ALIAS, arguments, new FunctionData(-1, emptyList()));
        List<TupleVector> batches = drain(it);

        assertEquals(1, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
        assertEquals(Set.of(456), collectColumnValues(batches, "key"));
    }

    @Test
    void test_runCommand_function()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        RunCommandFunction function = (RunCommandFunction) catalog.getTableFunction("runCommand");
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression(DATABASE), ExpressionTestUtils.createStringExpression("{\"dbStats\": 1}"));

        TupleIterator it = function.execute(context, CATALOG_ALIAS, arguments, new FunctionData(-1, emptyList()));
        List<TupleVector> batches = drain(it);

        assertEquals(1, batches.stream()
                .mapToInt(TupleVector::getRowCount)
                .sum());
        TupleVector v = batches.get(0);
        assertEquals(DATABASE, v.getColumn(indexOf(v, "db"))
                .getAny(0));
        assertEquals(1.0, v.getColumn(indexOf(v, "ok"))
                .getAny(0));
    }

    @Test
    void test_system_tables()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("tables"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        Set<Object> tables = collectColumnValues(batches, "name");
        assertTrue(tables.contains(DATABASE + "." + COLLECTION));
    }

    @Test
    void test_system_indices()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("indices"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        List<Object> tables = new ArrayList<>();
        for (TupleVector v : batches)
        {
            ValueVector tableCol = v.getColumn(indexOf(v, "table"));
            for (int i = 0; i < v.getRowCount(); i++)
            {
                tables.add(tableCol.getAny(i));
            }
        }
        // one row for the always present _id index, one for the secondary "key" index
        assertEquals(2, tables.size());
    }

    @Test
    void test_system_columns()
    {
        insertTestData();
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("columns"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        Set<Object> columns = new HashSet<>();
        for (TupleVector v : batches)
        {
            ValueVector tableCol = v.getColumn(indexOf(v, "table"));
            ValueVector nameCol = v.getColumn(indexOf(v, "name"));
            for (int i = 0; i < v.getRowCount(); i++)
            {
                if ((DATABASE + "." + COLLECTION).equals(tableCol.getAny(i)))
                {
                    columns.add(nameCol.getAny(i));
                }
            }
        }
        // Only '_id' and the indexed "key" field are reported - the non-indexed "name" field is intentionally not sampled/surfaced
        assertEquals(Set.of("_id", "key"), columns);
    }

    @Test
    void test_system_functions()
    {
        IExecutionContext context = mockContext();

        IDatasource ds = catalog.getSystemTableDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("functions"),
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));

        List<TupleVector> batches = drain(ds.execute(context));
        assertTrue(collectColumnValues(batches, "name").contains("aggregate"));
    }

    private ISeekPredicate mockSeekPredicate(IExecutionContext context, String column, Object... values)
    {
        QualifiedName table = QualifiedName.of(DATABASE, COLLECTION);
        TableSchema tableSchema = catalog.getTableSchema(context, CATALOG_ALIAS, table, emptyList());
        Index index = tableSchema.getIndices()
                .stream()
                .filter(i -> i.getColumns()
                        .stream()
                        .anyMatch(c -> c.equalsIgnoreCase(column)))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Index on " + column + " should exist"));

        ISeekPredicate seekPredicate = mock(ISeekPredicate.class);
        when(seekPredicate.getIndex()).thenReturn(index);
        when(seekPredicate.getIndexColumns()).thenReturn(asList(column));
        ISeekPredicate.ISeekKey seekKey = mock(ISeekPredicate.ISeekKey.class);
        when(seekPredicate.getSeekKeys(any(IExecutionContext.class))).thenReturn(asList(seekKey));
        when(seekKey.getValue()).thenReturn(VectorTestUtils.vv(Type.Any, values));
        return seekPredicate;
    }

    private static int indexOf(TupleVector vector, String column)
    {
        List<Column> columns = vector.getSchema()
                .getColumns();
        for (int i = 0; i < columns.size(); i++)
        {
            if (columns.get(i)
                    .getName()
                    .equalsIgnoreCase(column))
            {
                return i;
            }
        }
        throw new RuntimeException("Column " + column + " not found in schema " + vector.getSchema());
    }

    private static Set<Object> collectColumnValues(List<TupleVector> batches, String column)
    {
        Set<Object> result = new HashSet<>();
        for (TupleVector v : batches)
        {
            ValueVector vv = v.getColumn(indexOf(v, column));
            for (int i = 0; i < v.getRowCount(); i++)
            {
                result.add(vv.getAny(i));
            }
        }
        return result;
    }

    private static List<TupleVector> drain(TupleIterator it)
    {
        List<TupleVector> result = new ArrayList<>();
        while (it.hasNext())
        {
            result.add(it.next());
        }
        it.close();
        return result;
    }
}
