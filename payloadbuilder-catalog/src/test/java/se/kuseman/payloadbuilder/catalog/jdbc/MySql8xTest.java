package se.kuseman.payloadbuilder.catalog.jdbc;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import com.mysql.cj.jdbc.MysqlDataSource;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/** Test of MySql8.x */
public class MySql8xTest extends BaseJDBCTest
{
    public MySql8xTest()
    {
        super(Mysql.getDatasource(), Mysql.getUrl(), "com.mysql.cj.jdbc.Driver", "root", Mysql.PASSWORD);
    }

    @AfterClass
    public static void tearDownClass()
    {
        Mysql.stop();
    }

    @Override
    protected String getColumnDeclaration(Column column)
    {
        Type type = column.getType()
                .getType();
        if (type == Type.Float)
        {
            return "FLOAT";
        }
        else if (type == Type.Double)
        {
            return "DOUBLE";
        }
        else if (type == Type.DateTime)
        {
            return "DATETIME";
        }
        return super.getColumnDeclaration(column);
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        if (type == Type.Float)
        {
            precision = 12;
        }
        else if (type == Type.Double)
        {
            precision = 22;
        }
        else if (type == Type.DateTime)
        {
            precision = 19;
            scale = 0;
        }
        return super.getColumn(type, name, precision, scale);
    }

    static class Mysql
    {
        private static final String PASSWORD = "A_Str0ng_Required_Password";
        private static final int PORT = 3306;
        private static final String IMAGE_NAME = "mysql:8.0.33-debian";
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
            MysqlDataSource ds = new MysqlDataSource();
            ds.setUrl(getUrl());
            ds.setUser("root");
            ds.setPassword(PASSWORD);
            return ds;
        }

        static String getUrl()
        {
            return "jdbc:mysql://localhost:" + getPort();
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
