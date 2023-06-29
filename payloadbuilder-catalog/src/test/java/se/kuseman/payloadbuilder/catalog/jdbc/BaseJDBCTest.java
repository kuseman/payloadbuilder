package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockOptions;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.SeekType;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.test.IPredicateMock;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Base test class for {@link JdbcCatalog} */
// CSOFF
abstract class BaseJDBCTest extends Assert
// CSON
{
    private static final String CATALOG_ALIAS = "jdbc";
    protected static final String TEST_DB = "test_db";
    protected static final String TEST_TABLE = "test_table";

    private final DataSource datasource;
    private final String jdbcUrl;
    private final String driverClassName;
    private final String username;
    private final String password;
    private final JdbcCatalog catalog = new JdbcCatalog();

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

    @After
    public void shutdown()
    {
        catalog.shutdown();

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
        // Create test data
        try (Connection con = datasource.getConnection())
        {
            con.setCatalog(TEST_DB);
            con.prepareStatement("CREATE TABLE " + TEST_TABLE + " ( " + " col1 INT," + " col2 VARCHAR(100))")
                    .execute();

            //@formatter:off
            con.prepareStatement(
                    "INSERT INTO " + TEST_TABLE + " ( col1, col2 ) "
                            + "      SELECT 1, 'one' "
                            + "UNION SELECT 2, 'two' "
                            + "UNION SELECT 3, 'three' "
                            + "UNION SELECT 4, 'four' "
                            + "UNION SELECT 5, 'five' "
            )
            .execute();
            //@formatter:on
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test_datasource_table_scan_projection()
    {
        IExecutionContext context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE),
                new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), asList("col2")));
        TupleIterator it = ds.execute(context, mockOptions(500));

        List<String> col2Expected = asList("one", "two", "three", "four", "five");
        int expectedSize = col2Expected.size();

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());

            for (int i = 0; i < expectedSize; i++)
            {
                for (int j = 0; j < expectedSize; j++)
                {
                    if (col2Expected.get(i)
                            .equals(v.getColumn(0)
                                    .getAny(j)))
                    {
                        rowCount++;
                    }
                }
            }
        }
        it.close();

        assertEquals(expectedSize, rowCount);
    }

    @Test
    public void test_datasource_table_scan_asterisk()
    {
        IExecutionContext context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, Optional.empty(), emptyList(), emptyList(), emptyList()));
        TupleIterator it = ds.execute(context, mockOptions(500));

        List<Integer> col1Expected = asList(1, 2, 3, 4, 5);
        List<String> col2Expected = asList("one", "two", "three", "four", "five");
        int expectedSize = col1Expected.size();

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col1", ResolvedType.of(Type.Any)), Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());

            for (int i = 0; i < expectedSize; i++)
            {
                for (int j = 0; j < expectedSize; j++)
                {
                    if (col1Expected.get(i)
                            .equals(v.getColumn(0)
                                    .getAny(j))
                            && col2Expected.get(i)
                                    .equals(v.getColumn(1)
                                            .getAny(j)))
                    {
                        rowCount++;
                    }
                }
            }
        }
        it.close();

        assertEquals(expectedSize, rowCount);
    }

    @Test
    public void test_datasource_table_scan_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of("col1"), Order.DESC)));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, Optional.empty(), emptyList(), sortItems, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context, mockOptions(500));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col1", ResolvedType.of(Type.Any)), Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, 5, 4, 3, 2, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, "five", "four", "three", "two", "one"), v.getColumn(1));

            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(5, rowCount);
    }

    @Test
    public void test_datasource_table_scan_asterisk_with_sortitems_with_predicates()
    {
        IExecutionContext context = mockExecutionContext();
        List<ISortItem> sortItems = new ArrayList<>(asList(mockSortItem(QualifiedName.of("col1"), Order.DESC)));
        List<IPredicate> predicates = new ArrayList<>(asList(IPredicateMock.in("col2", asList("one", "four"))));
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of(TEST_TABLE), new DatasourceData(0, Optional.empty(), predicates, sortItems, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());
        assertTrue(predicates.isEmpty());

        TupleIterator it = ds.execute(context, mockOptions(500));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col1", ResolvedType.of(Type.Any)), Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, 4, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, "four", "one"), v.getColumn(1));

            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(2, rowCount);
    }

    @Test
    public void test_datasource_index_seek_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, asList("col1"), Arrays.<Object[]>asList(new Object[] { 1, 3, 5 }));
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of("col1"), Order.DESC)));
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, Optional.empty(), emptyList(), sortItems, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context, mockOptions(500));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col1", ResolvedType.of(Type.Any)), Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, 5, 3, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, "five", "three", "one"), v.getColumn(1));

            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(3, rowCount);
    }

    @Test
    public void test_datasource_index_seek_multiple_columns_asterisk_with_sortitems()
    {
        IExecutionContext context = mockExecutionContext();
        ISeekPredicate seekPredicate = mockSeekPrecidate(context, asList("col1", "col2"), Arrays.<Object[]>asList(new Object[] { 1, 3, 5 }, new Object[] { "one", "five", "three" }));
        List<ISortItem> sortItems = new ArrayList<>(asList(TestUtils.mockSortItem(QualifiedName.of("col1"), Order.DESC)));
        IDatasource ds = catalog.getSeekDataSource(context.getSession(), CATALOG_ALIAS, seekPredicate, new DatasourceData(0, Optional.empty(), emptyList(), sortItems, emptyList()));

        // Verify sort items consumed
        assertTrue(sortItems.isEmpty());

        TupleIterator it = ds.execute(context, mockOptions(500));

        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            assertEquals(Schema.of(Column.of("col1", ResolvedType.of(Type.Any)), Column.of("col2", ResolvedType.of(Type.Any))), v.getSchema());
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, 1), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(Type.Any, "one"), v.getColumn(1));

            rowCount += v.getRowCount();
        }
        it.close();

        assertEquals(1, rowCount);
    }

    private IExecutionContext mockExecutionContext()
    {
        //@formatter:off
         IExecutionContext context = TestUtils.mockExecutionContext(CATALOG_ALIAS, 
                 ofEntries(
                     entry(JdbcCatalog.URL             ,jdbcUrl),
                     entry(JdbcCatalog.DRIVER_CLASSNAME,driverClassName),
                     entry(JdbcCatalog.DATABASE        ,TEST_DB),
                     entry(JdbcCatalog.USERNAME        ,username),
                     entry(JdbcCatalog.PASSWORD        ,password)
                 ), 0, null);
         //@formatter:on
        return context;
    }

    private ISeekPredicate mockSeekPrecidate(IExecutionContext context, List<String> columns, List<Object[]> values)
    {
        // Fetch indices and extract the one on key field
        QualifiedName table = QualifiedName.of(TEST_TABLE);
        TableSchema tableSchema = catalog.getTableSchema(context.getSession(), CATALOG_ALIAS, table);
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
            when(seekKey.getType()).thenReturn(SeekType.EQ);
            when(seekKey.getValue()).thenReturn(ValueVector.literalAny(values.get(i)));
        }
        when(seekPredicate.getSeekKeys(any(IExecutionContext.class))).thenReturn(seekKeys);
        return seekPredicate;
    }
}
