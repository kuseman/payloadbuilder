package se.kuseman.payloadbuilder.api.execution;

import java.io.Writer;

/** Definition of a query session. */
public interface IQuerySession
{
    /** Get the current default catalog alias */
    String getDefaultCatalogAlias();

    /** Get catalog property */
    <T> T getCatalogProperty(String alias, String key);

    /** Get catalog property with default value support */
    default <T> T getCatalogProperty(String alias, String key, T defaultValue)
    {
        T val = getCatalogProperty(alias, key);
        if (val == null)
        {
            return defaultValue;
        }
        return val;
    }

    /** Set catalog property */
    void setCatalogProperty(String catalogAlias, String key, Object value);

    /** Get the generic cache */
    GenericCache getGenericCache();

    /** Returns true if the query should be aborted */
    boolean abortQuery();

    /** Get the print writer used for message outputs from session */
    Writer getPrintWriter();

    /** Return the execution time in ms. for the last executed query */
    long getLastQueryExecutionTime();
}
