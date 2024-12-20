package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.api.catalog.Column.MetaData.NULLABLE;
import static se.kuseman.payloadbuilder.api.catalog.Column.MetaData.PRECISION;
import static se.kuseman.payloadbuilder.api.catalog.Column.MetaData.SCALE;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertTupleVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Base test class for {@link JdbcCatalog} */
// CSOFF
abstract class BaseJDBCTest extends Assert
// CSON
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseJDBCTest.class);
    protected static final String CATALOG_ALIAS = "jdbc";
    protected static final String TEST_DB = "test_db";
    protected static final String TEST_TABLE = "test_table";

    private static final String VARCHAR100_COL = "varchar100Col";
    private static final String INT_COL = "intCol";
    private static final String LONG_COL = "longCol";
    private static final String BOOL_COL = "boolCol";
    private static final String FLOAT_COL = "floatCol";
    private static final String DOUBLE_COL = "doubleCol";
    private static final String DECIMAL_COL = "decimalCol";
    private static final String DATETIME_COL = "dateTimeCol";

    protected final DataSource datasource;
    private final String jdbcUrl;
    private final String driverClassName;
    private final String username;
    private final String password;
    protected final JdbcCatalog catalog = new JdbcCatalog();

    private Schema expectedFullSchema;
    private List<Column.Type> expectedFullSchemaTypes;

    BaseJDBCTest(DataSource datasource, String jdbcUrl, String driverClassName, String username, String password)
    {
        this.datasource = datasource;
        this.jdbcUrl = jdbcUrl;
        this.driverClassName = driverClassName;
        this.username = username;
        this.password = password;
    }

    protected static void createDb(DataSource datasource)
    {
        // Create test data
        try (Connection con = datasource.getConnection())
        {
            con.prepareStatement("CREATE DATABASE " + TEST_DB)
                    .execute();
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected String getColumnDeclaration(Column column)
    {
        switch (column.getType()
                .getType())
        {
            case Boolean:
                return "BIT";
            case Int:
                return "INT";
            case Long:
                return "BIGINT";
            case Float:
                return "REAL";
            case Double:
                return "FLOAT";
            case Decimal:
                return "NUMERIC(19,4)";
            case DateTime:
                return "TIMESTAMP";
            case String:
                return "VARCHAR(" + column.getMetaData()
                        .getPrecision()
                       + ")";
            default:
                throw new IllegalArgumentException("Unsupported type: " + column.getType());
        }
    }

    protected String getBooleanValue(boolean value)
    {
        return value ? "1"
                : "0";
    }

    protected String getTimestampValue(String timestamp)
    {
        return timestamp;
    }

    protected Column getBooleanColumn(String name)
    {
        return getColumn(Type.Boolean, name, 1, 0);
    }

    protected Column getIntColumn(String name)
    {
        return getColumn(Type.Int, name, 10, 0);
    }

    protected Column getLongColumn(String name)
    {
        return getColumn(Type.Long, name, 19, 0);
    }

    protected Column getFloatColumn(String name)
    {
        return getColumn(Type.Float, name, 7, 0);
    }

    protected Column getDoubleColumn(String name)
    {
        return getColumn(Type.Double, name, 15, 0);
    }

    protected Column getDecimalColumn(String name)
    {
        return getColumn(Type.Decimal, name, 19, 4);
    }

    protected Column getDateTimeColumn(String name)
    {
        return getColumn(Type.DateTime, name, 0, 6);
    }

    protected Column getStringColumn(String name, int precision)
    {
        return getColumn(Type.String, name, precision, 0);
    }

    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        switch (type)
        {
            case Boolean:
            case Int:
            case Long:
            case Float:
            case Double:
            case Decimal:
            case DateTime:
            case String:
                return Column.of(name, type, new Column.MetaData(Map.of(PRECISION, precision, NULLABLE, true, SCALE, scale)));
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    @After
    public void shutdown()
    {
        catalog.close();

        // Clean up test data
        try (Connection con = datasource.getConnection())
        {
            con.setCatalog(TEST_DB);
            con.prepareStatement("DROP TABLE " + TEST_TABLE)
                    .execute();
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before()
    {
        //@formatter:off
        List<Column> columns = List.of(
                getIntColumn(INT_COL),
                getStringColumn(VARCHAR100_COL, 100),
                getLongColumn(LONG_COL),
                getBooleanColumn(BOOL_COL),
                getFloatColumn(FLOAT_COL),
                getDoubleColumn(DOUBLE_COL),
                getDecimalColumn(DECIMAL_COL),
                getDateTimeColumn(DATETIME_COL));
        //@formatter:on
        // Create test data
        try (Connection con = datasource.getConnection())
        {
            con.setCatalog(TEST_DB);
            String createTableStm = columns.stream()
                    .map(c -> c.getName() + " " + getColumnDeclaration(c))
                    .collect(Collectors.joining(",\n", "CREATE TABLE " + TEST_TABLE + " (\n", "\n)"));
            LOGGER.info("Create test table:\n{}", createTableStm);

            con.prepareStatement(createTableStm)
                    .execute();

            //@formatter:off
            List<List<String>> columnValues = List.of(
                    List.of("1", "'one'",   "10", getBooleanValue(false), "10.10", "100.100", "1000.1111", getTimestampValue("'2010-07-10 10:10:10'")),
                    List.of("2", "'two'",   "20", getBooleanValue(true),  "20.22", "200.222", "2000.2222", getTimestampValue("'2011-08-11 11:11:11'")),
                    List.of("3", "'three'", "30", getBooleanValue(false), "30.33", "300.333", "3000.3333", getTimestampValue("'2012-09-12 12:12:12'")),
                    List.of("4", "'four'",  "40", getBooleanValue(true),  "40.44", "400.444", "4000.4444", getTimestampValue("'2013-10-13 13:13:13'")),
                    List.of("5", "'five'",  "50", getBooleanValue(false), "50.55", "500.555", "5000.5555", getTimestampValue("'2014-11-14 14:14:14'"))
                    );

            String stmRow = "INSERT INTO %s ( %s, %s, %s, %s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)";
            List<String> stms = columnValues.stream().map(l ->
            {
                List<Object> args = new ArrayList<>();
                args.addAll(List.of(TEST_TABLE, INT_COL, VARCHAR100_COL, LONG_COL, BOOL_COL, FLOAT_COL, DOUBLE_COL, DECIMAL_COL, DATETIME_COL));
                args.addAll(l);
                return stmRow.formatted(args.toArray());
            })
            .toList();
            LOGGER.info("Insert into test table:\n{}", String.join("\n", stms));
            //@formatter:on

            for (String s : stms)
            {
                con.prepareStatement(s)
                        .execute();
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }

        //@formatter:off
        expectedFullSchema = new Schema(columns);
        expectedFullSchemaTypes = expectedFullSchema.getColumns().stream().map(c -> c.getType().getType()).toList();
        //@formatter:on
    }

    @Test
    public void test_sql_server_warnings_exceptions()
    {
        // Multi statement queries is not supported by all rdbms:es
        assumeTrue(jdbcUrl.contains("sqlserver"));

        StringWriter writer = new StringWriter();
        IExecutionContext context = mockExecutionContext(writer);
        QueryFunction query = new QueryFunction(catalog);

        TupleIterator it = query.execute(context, CATALOG_ALIAS, asList(ExpressionTestUtils.createStringExpression("""
                raiserror('warn 0', 0, 2);       -- Warn level
                create table vals
                (
                    rowId int
                );
                raiserror('warn 1', 5, 2);       -- Warn level
                insert into vals values (1);
                raiserror('warn 2', 10, 2);      -- Warn level
                insert into vals values (2);
                select * from vals;
                raiserror('error 1', 15, 2);      -- Error level
                drop table vals;
                select 'done'
                """)

        ), new FunctionData(0, emptyList()));

        Column.MetaData stringMeta = new Column.MetaData(Map.of(NULLABLE, false, SCALE, 0, PRECISION, 4));
        Column.MetaData intMeta = getIntColumn("a").getMetaData();

        int batchCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            if (batchCount == 0)
            {
                assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("rowId", Type.Int, intMeta)), asList(vv(Type.Int, 1, 2))), next);
            }
            else
            {
                assertTupleVectorsEquals(TupleVector.of(Schema.of(Column.of("", Type.String, stringMeta)), asList(vv(Type.String, "done"))), next);
            }

            batchCount++;
        }
        it.close();
        assertEquals(2, batchCount);

        assertEquals("""
                warn 0
                0 row(s) affected
                warn 1
                1 row(s) affected
                warn 2
                1 row(s) affected
                error 1
                0 row(s) affected
                """.replaceAll("[\\n\\r]", System.lineSeparator()), writer.toString());
    }

    @Test
    public void test_sql_server_datetime_offset()
    {
        // Multi statement queries is not supported by all rdbms:es
        assumeTrue(jdbcUrl.contains("sqlserver"));

        IExecutionContext context = mockExecutionContext();
        QueryFunction query = new QueryFunction(catalog);

        TupleIterator it = query.execute(context, CATALOG_ALIAS, asList(ExpressionTestUtils.createStringExpression("""
                select cast('2010-10-10' as datetimeoffset) _do
                , cast('2010-10-10' as datetimeoffset) at time zone 'Central European Standard Time' _do1
                , cast(null as datetimeoffset) _do2
                """)

        ), new FunctionData(0, emptyList()));

        Column.MetaData datetimeOffsetMeta = new Column.MetaData(Map.of(NULLABLE, true, PRECISION, 34, SCALE, 7));

        int batchCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            //@formatter:off
            assertTupleVectorsEquals(
                    TupleVector.of(Schema.of(
                            Column.of("_do", Type.DateTimeOffset, datetimeOffsetMeta),
                            Column.of("_do1", Type.DateTimeOffset, datetimeOffsetMeta),
                            Column.of("_do2", Type.DateTimeOffset, datetimeOffsetMeta)),
                            asList(vv(Type.DateTimeOffset, "2010-10-10"),
                                    vv(Type.DateTimeOffset, "2010-10-10"),
                                    vv(Type.DateTimeOffset, new Object[] { null }))),
                    next);
            //@formatter:on

            batchCount++;
        }
        it.close();
        assertEquals(1, batchCount);
    }

    @Test
    public void test_datasource_table_scan_projection()
    {
        IExecutionContext context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE),
                new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(VARCHAR100_COL)), emptyList()));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(VARCHAR100_COL, 100);
        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(stringColumn), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                    .getType(), "one", "two", "three", "four", "five"), v.getColumn(0));
        }
        it.close();
    }

    @Test
    public void test_datasource_table_scan_with_projection_option_override_plb_type()
    {
        IExecutionContext context = mockExecutionContext();

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(DOUBLE_COL)),
                new Option(QualifiedName.of(JdbcCatalog.COLUMN, DOUBLE_COL, JdbcCatalog.PLBTYPE), new LiteralStringExpression("Int")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, options));
        TupleIterator it = ds.execute(context);

        // Type should have been switched to int with reset precision and scale
        Column intColumn = Column.of(getDoubleColumn(DOUBLE_COL).getName(), Type.Int, new Column.MetaData(Map.of(PRECISION, -1, NULLABLE, true, SCALE, -1)));
        assertTrue(it.hasNext());
        TupleVector v = it.next();
        assertEquals(Schema.of(intColumn), v.getSchema());
        assertVectorsEquals(VectorTestUtils.vv(getIntColumn(DOUBLE_COL).getType(), 100, 200, 300, 400, 500), v.getColumn(0));

        assertFalse(it.hasNext());
        it.close();
    }

    @Test
    public void test_datasource_table_scan_with_projection_option_batch_size()
    {
        IExecutionContext context = mockExecutionContext();

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(VARCHAR100_COL)), new Option(IExecutionContext.BATCH_SIZE, new LiteralIntegerExpression(3)));

        // Send in a PLB projection to verify that the table option wins
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE),
                new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(INT_COL)), options));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(VARCHAR100_COL, 100);
        assertTrue(it.hasNext());
        TupleVector v = it.next();
        assertEquals(Schema.of(stringColumn), v.getSchema());
        assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                .getType(), "one", "two", "three"), v.getColumn(0));

        assertTrue(it.hasNext());
        v = it.next();
        assertEquals(Schema.of(stringColumn), v.getSchema());
        assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                .getType(), "four", "five"), v.getColumn(0));

        assertFalse(it.hasNext());
        it.close();
    }

    @Test
    public void test_datasource_table_scan_with_projection_option()
    {
        IExecutionContext context = mockExecutionContext();

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(VARCHAR100_COL)));

        // Send in a PLB projection to verify that the table option wins
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE),
                new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(INT_COL)), options));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(VARCHAR100_COL, 100);
        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(stringColumn), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                    .getType(), "one", "two", "three", "four", "five"), v.getColumn(0));
        }
        it.close();
    }

    @Test
    public void test_datasource_table_scan_asterisk()
    {
        IExecutionContext context = mockExecutionContext();

        List<Option> options = emptyList();
        // Test table hints option
        if (jdbcUrl.contains("sqlserver"))
        {
            options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression("*")), new Option(JdbcCatalog.TABLE_HINTS, new LiteralStringExpression("WITH(NOLOCK)")));
        }

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, options));
        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            // CSOFF
            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 1, 2, 3, 4, 5), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "one", "two", "three", "four", "five"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 10, 20, 30, 40, 50), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, true, false, true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 10.10F, 20.22F, 30.33F, 40.44F, 50.55F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 100.100D, 200.222D, 300.333D, 400.444D, 500.555D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("1000.1111"), bd("2000.2222"), bd("3000.3333"), bd("4000.4444"), bd("5000.5555")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2010-07-10T10:10:10", "2011-08-11T11:11:11", "2012-09-12T12:12:12", "2013-10-13T13:13:13", "2014-11-14T14:14:14"),
                    v.getColumn(7));
            // CSON
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(5, rowCount);
    }

    @Test
    public void test_datasource_table_scan_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of(INT_COL), Order.DESC)));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            // CSOFF
            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 5, 4, 3, 2, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "five", "four", "three", "two", "one"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 50, 40, 30, 20, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, true, false, true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 50.55F, 40.44F, 30.33F, 20.22F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 500.555D, 400.444D, 300.333D, 200.222D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("5000.5555"), bd("4000.4444"), bd("3000.3333"), bd("2000.2222"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2014-11-14T14:14:14", "2013-10-13T13:13:13", "2012-09-12T12:12:12", "2011-08-11T11:11:11", "2010-07-10T10:10:10"),
                    v.getColumn(7));
            // CSON
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(5, rowCount);
    }

    @Test
    public void test_datasource_table_scan_asterisk_with_sortitems_with_predicates()
    {
        IExecutionContext context = mockExecutionContext();
        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of(INT_COL), Order.DESC)));
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.in(VARCHAR100_COL, asList("one", "four"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, predicates, sortItems, Projection.ALL, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());
        assertTrue(predicates.isEmpty());

        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 4, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "four", "one"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 40, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 40.44F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 400.444D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("4000.4444"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2013-10-13T13:13:13", "2010-07-10T10:10:10"), v.getColumn(7));
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_datasource_index_seek_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, asList(INT_COL), Arrays.<Object[]>asList(new Object[] { 1, 3, 5 }));
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of(INT_COL), Order.DESC)));
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 5, 3, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "five", "three", "one"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 50, 30, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, false, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 50.55F, 30.33F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 500.555D, 300.333D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("5000.5555"), bd("3000.3333"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2014-11-14T14:14:14", "2012-09-12T12:12:12", "2010-07-10T10:10:10"), v.getColumn(7));
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_index_seek_multiple_columns_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, asList(INT_COL, VARCHAR100_COL), Arrays.<Object[]>asList(new Object[] { 1, 3, 5 }, new Object[] { "one", "five", "three" }));
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of(INT_COL), Order.DESC)));
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "one"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 10L), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2010-07-10T10:10:10"), v.getColumn(7));
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    protected IExecutionContext mockExecutionContext()
    {
        return mockExecutionContext(null);
    }

    private IExecutionContext mockExecutionContext(Writer writer)
    {
        //@formatter:off
         IExecutionContext context = TestUtils.mockExecutionContext(CATALOG_ALIAS,
                 ofEntries(
                     entry(JdbcCatalog.URL             ,jdbcUrl),
                     entry(JdbcCatalog.DRIVER_CLASSNAME,driverClassName),
                     entry(JdbcCatalog.DATABASE        ,TEST_DB),
                     entry(JdbcCatalog.USERNAME        ,username),
                     entry(JdbcCatalog.PASSWORD        ,password)
                 ), 0, null, writer);
         //@formatter:on
        return context;
    }

    private BigDecimal bd(String value)
    {
        return new BigDecimal(value);
    }

    private ISeekPredicate mockSeekPrecidate(IExecutionContext context, List<String> columns, List<Object[]> values)
    {
        // Fetch indices and extract the one on key field
        QualifiedName table = QualifiedName.of(TEST_TABLE);
        TableSchema tableSchema = catalog.getTableSchema(context.getSession(), CATALOG_ALIAS, table, emptyList());
        Index index = tableSchema.getIndices()
                .get(0);

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
