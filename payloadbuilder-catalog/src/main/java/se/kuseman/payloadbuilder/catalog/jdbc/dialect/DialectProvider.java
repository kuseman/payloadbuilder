package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import static org.apache.commons.lang3.Strings.CI;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.catalog.jdbc.JdbcCatalog;

/** Provider for fetching a {@link SqlDialect} */
public final class DialectProvider
{
    private static final String ORACLE_KEY = "oracle";
    private static final String SQLSERVER_KEY = "sqlserver";
    private static final String MARIADB_KEY = "mariadb";
    private static final String MYSQL_KEY = "mysql";
    private static final String POSTGRE_KEY = "postgre";
    private static final SqlDialect BASE = new SqlDialect()
    {
    };
    private static final OracleDialect ORACLE = new OracleDialect();
    private static final SqlServerDialect SQLSERVER = new SqlServerDialect();
    private static final MySqlMariaDbDialect MYSQLMARIADB = new MySqlMariaDbDialect();
    private static final PostgreDialect POSTGRE = new PostgreDialect();

    /** Return a {@link SqlDialect} from provided jdbc url and driver class */
    public static SqlDialect getDialect(String url, String driverClass)
    {
        if (CI.contains(driverClass, ORACLE_KEY)
                || CI.contains(url, ORACLE_KEY))
        {
            return ORACLE;
        }
        else if (CI.contains(driverClass, SQLSERVER_KEY)
                || CI.contains(url, SQLSERVER_KEY))
        {
            return SQLSERVER;
        }
        else if (CI.contains(driverClass, MARIADB_KEY)
                || CI.contains(url, MARIADB_KEY)
                || CI.contains(driverClass, MYSQL_KEY)
                || CI.contains(url, MYSQL_KEY))
        {
            return MYSQLMARIADB;
        }
        else if (CI.contains(driverClass, POSTGRE_KEY)
                || CI.contains(url, POSTGRE_KEY))
        {
            return POSTGRE;
        }
        return BASE;
    }

    /** Return {@link SqlDialect} from provided session and catalog alias */
    public static SqlDialect getDialect(IQuerySession session, String catalogAlias)
    {
        String url = session.getCatalogProperty(catalogAlias, JdbcCatalog.URL)
                .valueAsString(0);
        String driverClassName = session.getCatalogProperty(catalogAlias, JdbcCatalog.DRIVER_CLASSNAME)
                .valueAsString(0);

        return getDialect(url, driverClassName);
    }
}
