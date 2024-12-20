package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/** Test of SqlServer 2017 */
public class SqlServer2017Test extends ASqlServerTest
{
    public SqlServer2017Test()
    {
        super(SqlServer.getDatasource(), SqlServer.getUrl(), "com.microsoft.sqlserver.jdbc.SQLServerDriver", "sa", SqlServer.PASSWORD);
    }

    @AfterClass
    public static void tearDownClass()
    {
        SqlServer.stop();
    }

    @Override
    public void before()
    {
        org.junit.Assume.assumeTrue(SqlServer.CONTAINER.isRunning());
        super.before();
    }

    @Override
    public void shutdown()
    {
        if (SqlServer.CONTAINER.isRunning())
        {
            super.shutdown();
        }
    }

    static class SqlServer
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 1433;
        private static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2017-latest";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv("SA_PASSWORD", PASSWORD)
                .withEnv("MSSQL_PID", "Developer")
                .withEnv("ACCEPT_EULA", "Y");

        static
        {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> CONTAINER.stop()));

            CONTAINER.setWaitStrategy(new SqlWaitStrategy(() -> getDatasource()));
            // CONTAINER.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*The tempdb database has [1-9]+.*"));
            CONTAINER.start();
            try
            {
                createDb(getDatasource());
            }
            catch (Throwable e)
            {
                System.err.println("Container did not start correctly: " + CONTAINER.getLogs());
            }
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

    static class SqlWaitStrategy extends AbstractWaitStrategy
    {
        final Supplier<DataSource> datasource;

        SqlWaitStrategy(Supplier<DataSource> datasource)
        {
            this.datasource = datasource;
        }

        @Override
        public void waitUntilReady(WaitStrategyTarget waitStrategyTarget)
        {
            // Initial sleep of 10 secs.
            try
            {
                Thread.sleep(10_000);
            }
            catch (InterruptedException e1)
            {
            }
            super.waitUntilReady(waitStrategyTarget);
        }

        @Override
        protected void waitUntilReady()
        {
            int tryCount = 5;
            try (Connection con = datasource.get()
                    .getConnection())
            {
                con.prepareCall("SELECT 1")
                        .execute();
            }
            catch (SQLException e)
            {
                tryCount--;
                if (tryCount <= 0)
                {
                    throw new RuntimeException("Container did not start: " + waitStrategyTarget.getLogs());
                }

                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e1)
                {
                }
            }
        }
    }
}
