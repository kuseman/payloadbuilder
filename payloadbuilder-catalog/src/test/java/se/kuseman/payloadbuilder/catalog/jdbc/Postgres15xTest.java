package se.kuseman.payloadbuilder.catalog.jdbc;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

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

    static class Postgres
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 5432;
        private static final String IMAGE_NAME = "postgres:15.3-bullseye";
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
