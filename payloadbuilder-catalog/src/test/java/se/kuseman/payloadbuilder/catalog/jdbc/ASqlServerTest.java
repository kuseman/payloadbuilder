package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.expression.LiteralArrayExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

abstract class ASqlServerTest extends BaseJDBCTest
{
    ASqlServerTest(DataSource datasource, String jdbcUrl, String driverClassName, String username, String password)
    {
        super(datasource, jdbcUrl, driverClassName, username, password);
    }

    @Test
    public void test_qualified_tables() throws SQLException
    {
        try (Connection con = datasource.getConnection())
        {
            con.setCatalog(TEST_DB);

            try (PreparedStatement stm = con.prepareStatement("""
                    CREATE SCHEMA test
                    """))
            {
                stm.execute();
            }
        }

        IExecutionContext context = mockExecutionContext();
        QualifiedName tableName = QualifiedName.of("test", "test_table");

        Column intColumn = getIntColumn("myint");
        Column myStringColumn = getStringColumn("mystring", 100);
        IDatasink sink = catalog.getInsertIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), emptyList()));
        //@formatter:off
        sink.execute(context, TupleIterator.singleton(TupleVector.of(Schema.of(
                intColumn,
                myStringColumn),
                VectorTestUtils.vv(Type.Int, 10, 20),
                VectorTestUtils.vv(Type.String, "some value", "some other value"))));
        //@formatter:on

        StringWriter sw = new StringWriter();
        context = mockExecutionContext(sw);

        TableFunctionInfo function = catalog.getTableFunction("query");
        TupleIterator it = function.execute(context, CATALOG_ALIAS, List.of(new LiteralStringExpression("""
                INSERT INTO test.test_table VALUES (?, ?)
                """), new LiteralArrayExpression(VectorTestUtils.vv(Type.Any, 30, "some third value"))), new FunctionData(0, emptyList()));
        assertFalse(it.hasNext());
        assertTrue(sw.toString(), sw.toString()
                .contains("1 row(s) affected"));

        context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, tableName, new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));
        it = ds.execute(context);

        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(intColumn, myStringColumn), v.getSchema());
            //@formatter:off
            assertVectorsEquals(VectorTestUtils.vv(intColumn.getType().getType(), 10, 20, 30), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(myStringColumn.getType().getType(), "some value", "some other value", "some third value"), v.getColumn(1));
            //@formatter:on
        }
        it.close();
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        if (type == Type.DateTime)
        {
            precision = 27;
            scale = 7;
        }
        else if (type == Type.DateTimeOffset)
        {
            precision = 34;
            scale = 7;
        }
        return super.getColumn(type, name, precision, scale);
    }
}
