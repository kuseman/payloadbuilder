package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import oracle.jdbc.datasource.impl.OracleDataSource;

/** Test of Oracle21.x */
@Ignore
public class Oracle21xTest extends BaseJDBCTest
{
    public Oracle21xTest()
    {
        super(Oracle.getDatasource(), Oracle.getUrl(), "oracle.jdbc.OracleDriver", "user", Oracle.PASSWORD);
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
                .withEnv("APP_USER", "user")
                .withEnv("APP_USER_PASSWORD", PASSWORD);

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
            try
            {
                OracleDataSource ds = new OracleDataSource();
                ds.setURL(getUrl());
                ds.setUser("user");
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
