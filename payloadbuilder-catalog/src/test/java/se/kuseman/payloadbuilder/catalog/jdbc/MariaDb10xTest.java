package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/** Test of MariaDb10.x */
public class MariaDb10xTest extends BaseJDBCTest
{
    public MariaDb10xTest()
    {
        super(MariaDb.getDatasource(), MariaDb.getUrl(), "org.mariadb.jdbc.Driver", "root", MariaDb.PASSWORD);
    }

    @AfterClass
    public static void tearDownClass()
    {
        MariaDb.stop();
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        if (type == Type.Int)
        {
            precision = 11;
        }
        else if (type == Type.Long)
        {
            precision = 20;
        }
        else if (type == Type.Float)
        {
            scale = 31;
            precision = 12;
        }
        else if (type == Type.Double)
        {
            scale = 31;
            precision = 22;
        }
        else if (type == Type.DateTime)
        {
            precision = 19;
            scale = 0;
        }
        else if (type == Type.DateTimeOffset)
        {
            type = Type.DateTime;
            precision = 19;
            scale = 0;
        }
        return super.getColumn(type, name, precision, scale);
    }

    /** Return the UTC in LocaleDateTime since this dialect don't support offsets. */
    @Override
    protected Object zdt(String value)
    {
        ZonedDateTime zdt = (ZonedDateTime) super.zdt(value);
        return zdt.withZoneSameInstant(ZoneId.of("UTC"))
                .toLocalDateTime();
    }

    static class MariaDb
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 3306;
        private static final String IMAGE_NAME = "mariadb:10.6.14-focal";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
                .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD);

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
                MariaDbDataSource ds = new MariaDbDataSource();
                ds.setUrl(getUrl());
                ds.setUser("root");
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
            return "jdbc:mariadb://localhost:" + getPort();
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
