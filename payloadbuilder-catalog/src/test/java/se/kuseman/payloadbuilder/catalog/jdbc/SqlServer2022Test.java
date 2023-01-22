package se.kuseman.payloadbuilder.catalog.jdbc;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import se.kuseman.payloadbuilder.catalog.jdbc.SqlServer2017Test.SqlWaitStrategy;

/** Test of SqlServer 2022 */
public class SqlServer2022Test extends BaseJDBCTest
{
    public SqlServer2022Test()
    {
        super(SqlServer.getDatasource(), SqlServer.getUrl(), "com.microsoft.sqlserver.jdbc.SQLServerDriver", "sa", SqlServer.PASSWORD);
    }

    @AfterClass
    public static void tearDownClass()
    {
        SqlServer.stop();
    }

    static class SqlServer
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 1433;
        private static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2022-latest";
        private static final GenericContainer<?> CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv("SA_PASSWORD", PASSWORD)
                .withEnv("MSSQL_PID", "Developer")
                .withEnv("ACCEPT_EULA", "Y");

        static
        {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> CONTAINER.stop()));

            CONTAINER.setWaitStrategy(new SqlWaitStrategy(() -> getDatasource()));
            CONTAINER.start();
            createDb(getDatasource());
        }

        static DataSource getDatasource()
        {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(getUrl());
            ds.setUser("sa");
            ds.setPassword(PASSWORD);
            return ds;
        }

        static String getUrl()
        {
            return "jdbc:sqlserver://localhost:" + getPort() + ";encrypt=false;trustServerCertificate=true";
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
