package se.kuseman.payloadbuilder.api.session;

import java.io.Writer;

/** Definition of a query session. */
public interface IQuerySession
{
    /** Get the current default catalog alias */
    String getDefaultCatalogAlias();

    /** Get catalog property */
    <T> T getCatalogProperty(String alias, String key);

    /** Set catalog property */
    void setCatalogProperty(String catalogAlias, String key, Object value);

    /** Get the custom cache provider */
    CustomCacheProvider getCustomCacheProvider();

    /** Returns true if the query should be aborted */
    boolean abortQuery();

    /** Get the print writer used for message outputs from session */
    Writer getPrintWriter();
}
