package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

import oracle.jdbc.datasource.impl.OracleDataSource;

/** Test of Oracle21.x */
public class Oracle21xTest extends BaseJDBCTest
{
    public Oracle21xTest()
    {
        super(Oracle.getDatasource(), Oracle.getUrl(), "oracle.jdbc.OracleDriver", TEST_DB, Oracle.PASSWORD);
    }

    @Override
    protected String getTimestampValue(String timestamp)
    {
        return "TO_DATE(%s,'YYYY-MM-DD HH24:MI:SS')".formatted(timestamp);
    }

    @Override
    protected String getColumnDeclaration(Column column)
    {
        Type type = column.getType()
                .getType();
        if (type == Type.Int)
        {
            return "NUMBER(" + column.getMetaData()
                    .getPrecision()
                   + ")";
        }
        else if (type == Type.Long)
        {
            return "NUMBER(19)";
        }
        else if (type == Type.Boolean)
        {
            return "NUMBER(1)";
        }
        return super.getColumnDeclaration(column);
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        // Oracle doesn't have any dedicated boolean columns so use Int
        if (type == Type.Boolean)
        {
            type = Type.Int;
        }
        else if (type == Type.Float)
        {
            scale = -127;
            precision = 63;
        }
        else if (type == Type.Double)
        {
            scale = -127;
            precision = 126;
        }
        return super.getColumn(type, name.toUpperCase(), precision, scale);
    }

    @Test
    public void test_clob() throws SQLException
    {
        try (Connection con = datasource.getConnection())
        {
            try (PreparedStatement stm = con.prepareStatement("""
                    CREATE TABLE clob_test
                    (
                        MYCLOB  CLOB
                    ,   MYNCLOB NCLOB
                    )
                    """))
            {
                stm.execute();
            }

            try (PreparedStatement stm = con.prepareStatement("""
                    INSERT INTO clob_test (myclob, mynclob) VALUES ('clob', 'nclob')
                    """))
            {
                stm.execute();
            }
        }

        IExecutionContext context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("clob_test"), new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));
        TupleIterator it = ds.execute(context);

        Column clobColumn = getStringColumn("myclob", -1);
        Column nclobColumn = getStringColumn("mynclob", -1);

        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(clobColumn, nclobColumn), v.getSchema());
            //@formatter:off
            assertVectorsEquals(VectorTestUtils.vv(clobColumn.getType().getType(), "clob"), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(clobColumn.getType().getType(), "nclob"), v.getColumn(1));
            //@formatter:on
        }
        it.close();
    }

    @AfterClass
    public static void tearDownClass()
    {
        Oracle.stop();
    }

    static class Oracle
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 1521;
        private static final String IMAGE_NAME = "gvenzl/oracle-xe:21-slim";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv("ORACLE_RANDOM_PASSWORD", "true")
                .withEnv("APP_USER", TEST_DB)
                .withEnv("APP_USER_PASSWORD", PASSWORD);

        static
        {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> CONTAINER.stop()));

            CONTAINER.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*DATABASE IS READY TO USE.*"));
            CONTAINER.start();
        }

        static DataSource getDatasource()
        {
            try
            {
                OracleDataSource ds = new OracleDataSource();
                ds.setURL(getUrl());
                ds.setUser(TEST_DB);
                ds.setPassword(PASSWORD);
                return ds;
            }
            catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }

        static String getUrl()
        {
            return "jdbc:oracle:thin:@localhost:" + getPort() + "/XEPDB1";
        }

        static int getPort()
        {
            return CONTAINER.getMappedPort(PORT);
        }

        static void stop()
        {
            if (CONTAINER != null)
            {
                CONTAINER.stop();
            }
        }
    }

}
