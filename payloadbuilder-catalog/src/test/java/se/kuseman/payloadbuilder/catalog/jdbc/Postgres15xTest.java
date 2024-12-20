package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
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

/** Test of Postgres15.x */
public class Postgres15xTest extends BaseJDBCTest
{
    public Postgres15xTest()
    {
        super(Postgres.getDatasource(), Postgres.getUrl(), "org.postgresql.Driver", "root", Postgres.PASSWORD);
    }

    @AfterClass
    public static void tearDownClass()
    {
        Postgres.stop();
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        if (type == Type.Float)
        {
            precision = 8;
            scale = 8;
        }
        else if (type == Type.Double)
        {
            precision = 17;
            scale = 17;
        }
        else if (type == Type.DateTime)
        {
            precision = 29;
            scale = 6;
        }
        return super.getColumn(type, name.toLowerCase(), precision, scale);
    }

    @Override
    protected String getBooleanValue(boolean value)
    {
        return Boolean.toString(value);
    }

    @Override
    protected String getColumnDeclaration(Column column)
    {
        Type type = column.getType()
                .getType();
        if (type == Type.Boolean)
        {
            return "BOOLEAN";
        }
        return super.getColumnDeclaration(column);
    }

    @Test
    public void test_special_types() throws SQLException
    {
        try (Connection con = datasource.getConnection())
        {
            try (PreparedStatement stm = con.prepareStatement("""
                    CREATE TABLE json_test
                    (
                        myjson JSON
                    ,   myxml  XML
                    ,   myuuid UUID
                    )
                    """))
            {
                stm.execute();
            }

            try (PreparedStatement stm = con.prepareStatement("""
                    INSERT INTO json_test (myjson, myxml, myuuid) VALUES ('{"key": 123, "key2": "value"}', '<record id="123"></record>', 'c2d29867-3d0b-d497-9191-18a9d8ee7830')
                    """))
            {
                stm.execute();
            }
        }

        IExecutionContext context = mockExecutionContext();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("json_test"), new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList()));
        TupleIterator it = ds.execute(context);

        Column jsonColumn = getStringColumn("myjson", 2147483647);
        Column xmlColumn = getStringColumn("myxml", 2147483647);
        Column uuidColumn = getStringColumn("myuuid", 2147483647);

        while (it.hasNext())
        {
            TupleVector v = it.next();
            assertEquals(Schema.of(jsonColumn, xmlColumn, uuidColumn), v.getSchema());
            //@formatter:off
            assertVectorsEquals(VectorTestUtils.vv(jsonColumn.getType().getType(), "{\"key\": 123, \"key2\": \"value\"}"), v.getColumn(0));
            assertVectorsEquals(VectorTestUtils.vv(jsonColumn.getType().getType(), "<record id=\"123\"></record>"), v.getColumn(1));
            assertVectorsEquals(VectorTestUtils.vv(jsonColumn.getType().getType(), "c2d29867-3d0b-d497-9191-18a9d8ee7830"), v.getColumn(2));
            //@formatter:on
        }
        it.close();
    }

    static class Postgres
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 5432;
        private static final String IMAGE_NAME = "postgres:15.3-bullseye";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withEnv("POSTGRES_USER", "root");

        static
        {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> CONTAINER.stop()));

            CONTAINER.setWaitStrategy(new HostPortWaitStrategy());
            CONTAINER.start();
            createDb(getDatasource());
        }

        static DataSource getDatasource()
        {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setUrl(getUrl());
            ds.setUser("root");
            ds.setPassword(PASSWORD);
            return ds;
        }

        static String getUrl()
        {
            return "jdbc:postgresql://localhost:" + getPort() + "/";
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
