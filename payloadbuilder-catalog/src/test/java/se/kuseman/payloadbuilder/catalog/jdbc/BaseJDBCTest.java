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
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.SelectIntoData;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

import it.unimi.dsi.fastutil.ints.IntArrays;

/** Base test class for {@link JdbcCatalog} */
// CSOFF
abstract class BaseJDBCTest extends Assert
// CSON
{
    protected static final String CATALOG_ALIAS = "jdbc";
    protected static final String TEST_DB = "test_db";
    protected static final QualifiedName TEST_TABLE = QualifiedName.of("test_table");

    private static final String STRING100_COL = "string100Col";
    private static final String STRINGMAX_COL = "stringMaxCol";
    private static final String INT_COL = "intCol";
    private static final String LONG_COL = "longCol";
    private static final String BOOL_COL = "boolCol";
    private static final String FLOAT_COL = "floatCol";
    private static final String DOUBLE_COL = "doubleCol";
    private static final String DECIMAL_COL = "decimalCol";
    private static final String DATETIME_COL = "dateTimeCol";
    private static final String DATETIMEOFFSET_COL = "dateTimeOffsetCol";

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

    protected Column getDateTimeOffsetColumn(String name)
    {
        return getColumn(Type.DateTimeOffset, name, 0, 6);
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
            case DateTimeOffset:
            case String:
                return Column.of(name, type, new Column.MetaData(Map.of(PRECISION, precision, NULLABLE, true, SCALE, scale)));
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    @After
    public void shutdown()
    {
        IExecutionContext context = mockExecutionContext();
        catalog.dropTable(context.getSession(), CATALOG_ALIAS, TEST_TABLE, false);
        catalog.close();
    }

    @Before
    public void before()
    {
        //@formatter:off
        List<Column> columns = List.of(
                getIntColumn(INT_COL),
                getStringColumn(STRING100_COL, 100),
                getLongColumn(LONG_COL),
                getBooleanColumn(BOOL_COL),
                getFloatColumn(FLOAT_COL),
                getDoubleColumn(DOUBLE_COL),
                getDecimalColumn(DECIMAL_COL),
                getDateTimeColumn(DATETIME_COL),
                getDateTimeOffsetColumn(DATETIMEOFFSET_COL),
                getStringColumn(STRINGMAX_COL, -1)
                );

        expectedFullSchema = new Schema(columns);
        expectedFullSchemaTypes = expectedFullSchema.getColumns().stream().map(c -> c.getType().getType()).toList();
        //@formatter:on
        // Create test data

        IExecutionContext context = mockExecutionContext();
        IDatasink selectIntoSink = catalog.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new SelectIntoData(0, emptyList(), emptyList()));

        IExecutionContext executionContext = mockExecutionContext();

        // CSOFF
        //@formatter:off
        Schema testDataSchema = Schema.of(
                Column.of(INT_COL, Type.Int),
                Column.of(STRING100_COL, Type.String, new Column.MetaData(Map.of(MetaData.PRECISION, 100))),
                Column.of(LONG_COL, Type.Long),
                Column.of(BOOL_COL, Type.Boolean),
                Column.of(FLOAT_COL, Type.Float),
                Column.of(DOUBLE_COL, Type.Double),
                Column.of(DECIMAL_COL, Type.Decimal, new Column.MetaData(Map.of(MetaData.PRECISION, 19, MetaData.SCALE, 4))),
                Column.of(DATETIME_COL, Type.DateTime),
                Column.of(DATETIMEOFFSET_COL, Type.DateTimeOffset),
                Column.of(STRINGMAX_COL, Type.String, new Column.MetaData(Map.of(MetaData.PRECISION, -1)))
                );

        TupleVector testData = TupleVector.of(testDataSchema, List.of(
                VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5),
                VectorTestUtils.vv(Type.String, "oöe", "two", "three", "four", "five"),
                VectorTestUtils.vv(Type.Long, 10L, 20L, 30L, 40L, 50L),
                VectorTestUtils.vv(Type.Boolean, false, true, false, true, false),
                VectorTestUtils.vv(Type.Float, 10.10F, 20.22F, 30.33F, 40.44F, 50.55F),
                VectorTestUtils.vv(Type.Double, 100.100F, 200.222F, 300.333F, 400.444F, 500.555F),
                VectorTestUtils.vv(Type.Decimal, bd("1000.1111"), bd("2000.2222"), bd("3000.3333"), bd("4000.4444"), bd("5000.5555")),
                VectorTestUtils.vv(Type.DateTime, ldt("2010-07-10T10:10:10"), ldt("2011-08-11T11:11:11"), ldt("2012-09-12T12:12:12"), ldt("2013-10-13T13:13:13"), ldt("2014-11-14T14:14:14")),
                VectorTestUtils.vv(Type.DateTimeOffset, zdt("2010-07-10T10:10:10+00:00"), zdt("2011-08-11T11:11:11+01:00"), zdt("2012-09-12T12:12:12+02:00"), zdt("2013-10-13T13:13:13+03:00"), zdt("2014-11-14T14:14:14+04:00")),
                VectorTestUtils.vv(Type.String, "m_oöe", "m_two", "m_three", "m_four", "m_five")
                ));
        //@formatter:on
        // CSON

        selectIntoSink.execute(executionContext, TupleIterator.singleton(testData));
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
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE,
                new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(STRING100_COL)), emptyList()));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(STRING100_COL, 100);
        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(stringColumn), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                    .getType(), "oöe", "two", "three", "four", "five"), v.getColumn(0));
        }
        it.close();
    }

    @Test
    public void test_datasource_table_scan_with_projection_option_override_plb_type()
    {
        IExecutionContext context = mockExecutionContext();

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(DOUBLE_COL)),
                new Option(QualifiedName.of(JdbcCatalog.COLUMN, DOUBLE_COL, JdbcCatalog.PLBTYPE), new LiteralStringExpression("Int")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, options));
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

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(STRING100_COL)), new Option(IExecutionContext.BATCH_SIZE, new LiteralIntegerExpression(3)));

        // Send in a PLB projection to verify that the table option wins
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(INT_COL)), options));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(STRING100_COL, 100);
        assertTrue(it.hasNext());
        TupleVector v = it.next();
        assertEquals(Schema.of(stringColumn), v.getSchema());
        assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                .getType(), "oöe", "two", "three"), v.getColumn(0));

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

        List<Option> options = List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression(STRING100_COL)));

        // Send in a PLB projection to verify that the table option wins
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, emptyList(), emptyList(), Projection.columns(asList(INT_COL)), options));
        TupleIterator it = ds.execute(context);

        Column stringColumn = getStringColumn(STRING100_COL, 100);
        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(stringColumn), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(stringColumn.getType()
                    .getType(), "oöe", "two", "three", "four", "five"), v.getColumn(0));
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

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, options));
        TupleIterator it = ds.execute(context);

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            // CSOFF
            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 1, 2, 3, 4, 5), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "oöe", "two", "three", "four", "five"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 10, 20, 30, 40, 50), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, true, false, true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 10.10F, 20.22F, 30.33F, 40.44F, 50.55F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 100.100D, 200.222D, 300.333D, 400.444D, 500.555D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("1000.1111"), bd("2000.2222"), bd("3000.3333"), bd("4000.4444"), bd("5000.5555")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2010-07-10T10:10:10", "2011-08-11T11:11:11", "2012-09-12T12:12:12", "2013-10-13T13:13:13", "2014-11-14T14:14:14"),
                    v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2010-07-10T10:10:10+00:00"), zdt("2011-08-11T11:11:11+01:00"), zdt("2012-09-12T12:12:12+02:00"),
                    zdt("2013-10-13T13:13:13+03:00"), zdt("2014-11-14T14:14:14+04:00")), v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_oöe", "m_two", "m_three", "m_four", "m_five"), v.getColumn(9));
            // CSON
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(5, rowCount);

        // Insert some new rows
        IDatasink insertIntoSink = catalog.getInsertIntoSink(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new InsertIntoData(0, emptyList(), emptyList(), List.of(INT_COL, DECIMAL_COL)));

        // CSOFF
        //@formatter:off
        Schema testDataSchema = Schema.of(
                Column.of(INT_COL, Type.Int),
                Column.of(DECIMAL_COL, Type.Decimal, new Column.MetaData(Map.of(MetaData.PRECISION, 19, MetaData.SCALE, 4)))
                );

        TupleVector testData = TupleVector.of(testDataSchema, List.of(
                VectorTestUtils.vv(Type.Int, 10, null, 30),
                VectorTestUtils.vv(Type.Decimal, bd("666.1111"), bd("1337.2222"), null)
                ));
        //@formatter:on
        // CSON
        context = mockExecutionContext();
        insertIntoSink.execute(context, TupleIterator.singleton(testData));

        it = ds.execute(context);

        rowCount = 0;
        while (it.hasNext())
        {
            // Sort the vector since the order is inconsistent cross rdbms'es regarding nulls.
            TupleVector v = sort(it.next(), 0);
            // CSOFF
            assertEquals(expectedFullSchema, v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(0), 1, 2, 3, 4, 5, 10, 30, null), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "oöe", "two", "three", "four", "five", null, null, null), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 10, 20, 30, 40, 50, null, null, null), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, true, false, true, false, null, null, null), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 10.10F, 20.22F, 30.33F, 40.44F, 50.55F, null, null, null), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 100.100D, 200.222D, 300.333D, 400.444D, 500.555D, null, null, null), v.getColumn(5));
            assertVectorsEquals(
                    VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("1000.1111"), bd("2000.2222"), bd("3000.3333"), bd("4000.4444"), bd("5000.5555"), bd("666.1111"), null, bd("1337.2222")),
                    v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2010-07-10T10:10:10", "2011-08-11T11:11:11", "2012-09-12T12:12:12", "2013-10-13T13:13:13", "2014-11-14T14:14:14",
                    null, null, null), v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2010-07-10T10:10:10+00:00"), zdt("2011-08-11T11:11:11+01:00"), zdt("2012-09-12T12:12:12+02:00"),
                    zdt("2013-10-13T13:13:13+03:00"), zdt("2014-11-14T14:14:14+04:00"), null, null, null), v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_oöe", "m_two", "m_three", "m_four", "m_five", null, null, null), v.getColumn(9));
            // CSON
            rowCount += v.getRowCount();
        }
        it.close();
        assertEquals(8, rowCount);
    }

    private TupleVector sort(TupleVector vector, int col)
    {
        final ValueVector colA = vector.getColumn(col);
        final ValueVector colB = vector.getColumn(col);
        final Type type = colA.type()
                .getType();
        int[] indices = IntStream.range(0, vector.getRowCount())
                .toArray();
        IntArrays.mergeSort(indices, (a, b) ->
        {
            boolean aIsNull = colA.isNull(a);
            boolean bIsNull = colB.isNull(b);

            if (aIsNull
                    || bIsNull)
            {
                return (aIsNull ? 1
                        : 0)
                        - (bIsNull ? 1
                                : 0);
            }
            return VectorUtils.compare(colA, colB, type, a, b);
        });

        return new TupleVector()
        {
            @Override
            public int getRowCount()
            {
                return vector.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return new ValueVectorAdapter(vector.getColumn(column))
                {
                    @Override
                    protected int getRow(int row)
                    {
                        return indices[row];
                    }
                };
            }

            @Override
            public Schema getSchema()
            {
                return vector.getSchema();
            }
        };
    }

    @Test
    public void test_datasource_table_scan_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of(INT_COL), Order.DESC)));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, emptyList(), sortItems, Projection.ALL, emptyList()));

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
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "five", "four", "three", "two", "oöe"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 50, 40, 30, 20, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, true, false, true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 50.55F, 40.44F, 30.33F, 20.22F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 500.555D, 400.444D, 300.333D, 200.222D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("5000.5555"), bd("4000.4444"), bd("3000.3333"), bd("2000.2222"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2014-11-14T14:14:14", "2013-10-13T13:13:13", "2012-09-12T12:12:12", "2011-08-11T11:11:11", "2010-07-10T10:10:10"),
                    v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2014-11-14T14:14:14+04:00"), zdt("2013-10-13T13:13:13+03:00"), zdt("2012-09-12T12:12:12+02:00"),
                    zdt("2011-08-11T11:11:11+01:00"), zdt("2010-07-10T10:10:10+00:00")), v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_five", "m_four", "m_three", "m_two", "m_oöe"), v.getColumn(9));
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
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.in(STRING100_COL, asList("oöe", "four"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, TEST_TABLE, new DatasourceData(0, predicates, sortItems, Projection.ALL, emptyList()));

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
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "four", "oöe"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 40, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), true, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 40.44F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 400.444D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("4000.4444"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2013-10-13T13:13:13", "2010-07-10T10:10:10"), v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2013-10-13T13:13:13+03:00"), zdt("2010-07-10T10:10:10+00:00")), v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_four", "m_oöe"), v.getColumn(9));
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
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "five", "three", "oöe"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 50, 30, 10), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false, false, false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 50.55F, 30.33F, 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 500.555D, 300.333D, 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("5000.5555"), bd("3000.3333"), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2014-11-14T14:14:14", "2012-09-12T12:12:12", "2010-07-10T10:10:10"), v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2014-11-14T14:14:14+04:00"), zdt("2012-09-12T12:12:12+02:00"), zdt("2010-07-10T10:10:10+00:00")),
                    v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_five", "m_three", "m_oöe"), v.getColumn(9));
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_index_seek_multiple_columns_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, asList(INT_COL, STRING100_COL), Arrays.<Object[]>asList(new Object[] { 1, 3, 5 }, new Object[] { "oöe", "five", "three" }));
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
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(1), "oöe"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(2), 10L), v.getColumn(2));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(3), false), v.getColumn(3));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(4), 10.10F), v.getColumn(4));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(5), 100.100D), v.getColumn(5));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(6), bd("1000.1111")), v.getColumn(6));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(7), "2010-07-10T10:10:10"), v.getColumn(7));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(8), zdt("2010-07-10T10:10:10+00:00")), v.getColumn(8));
            assertVectorsEquals(VectorTestUtils.vv(expectedFullSchemaTypes.get(9), "m_oöe"), v.getColumn(9));
            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    protected IExecutionContext mockExecutionContext()
    {
        return mockExecutionContext(null);
    }

    protected IExecutionContext mockExecutionContext(Writer writer)
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

    private LocalDateTime ldt(String value)
    {
        return LocalDateTime.parse(value);
    }

    protected Object zdt(String value)
    {
        return ZonedDateTime.parse(value);
    }

    private ISeekPredicate mockSeekPrecidate(IExecutionContext context, List<String> columns, List<Object[]> values)
    {
        // Fetch indices and extract the one on key field
        TableSchema tableSchema = catalog.getTableSchema(context.getSession(), CATALOG_ALIAS, TEST_TABLE, emptyList());
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
