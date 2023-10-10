package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import oracle.jdbc.datasource.impl.OracleDataSource;

/** Test of Oracle21.x */
public class Oracle21xTest extends BaseJDBCTest
{
    public Oracle21xTest()
    {
        super(Oracle.getDatasource(), Oracle.getUrl(), "oracle.jdbc.OracleDriver", TEST_DB, Oracle.PASSWORD);
    }

    @Override
    protected String getColumn(String name)
    {
        return name.toUpperCase();
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
