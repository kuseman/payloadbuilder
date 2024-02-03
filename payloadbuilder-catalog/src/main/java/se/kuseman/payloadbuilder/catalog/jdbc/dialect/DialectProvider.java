package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.catalog.jdbc.JdbcCatalog;

/** Provider for fetching a {@link SqlDialect} */
public final class DialectProvider
{
    private static final String ORACLE_KEY = "oracle";
    private static final SqlDialect BASE = new SqlDialect()
    {
    };
    private static final OracleDialect ORACLE = new OracleDialect();

    /** Return a {@link SqlDialect} from provided jdbc url and driver class */
    public static SqlDialect getDialect(String url, String driverClass)
    {
        if (isBlank(url)
                || isBlank(driverClass))
        {
            return BASE;
        }
        else if (StringUtils.containsIgnoreCase(driverClass, ORACLE_KEY)
                || StringUtils.containsAnyIgnoreCase(url, ORACLE_KEY))
        {
            return ORACLE;
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
