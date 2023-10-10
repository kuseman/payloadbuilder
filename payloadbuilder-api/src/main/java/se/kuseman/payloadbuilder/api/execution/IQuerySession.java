package se.kuseman.payloadbuilder.api.execution;

import java.io.IOException;
import java.io.Writer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Definition of a query session. */
public interface IQuerySession
{
    /** Get the current default catalog alias */
    String getDefaultCatalogAlias();

    /** Get catalog property. If no value is found a null {@link ValueVector} is returned */
    ValueVector getCatalogProperty(String alias, String key);

    /** Get catalog property with default value support */
    default ValueVector getCatalogProperty(String alias, String key, ValueVector defaultValue)
    {
        ValueVector val = getCatalogProperty(alias, key);
        if (val == null
                || val.isNull(0))
        {
            return defaultValue;
        }
        return val;
    }

    /** Set catalog property */
    void setCatalogProperty(String catalogAlias, String key, ValueVector value);

    /** Set string catalog property */
    default void setCatalogProperty(String catalogAlias, String key, String value)
    {
        setCatalogProperty(catalogAlias, key, value == null ? ValueVector.literalNull(ResolvedType.of(Type.Any), 1)
                : ValueVector.literalString(value, 1));
    }

    /** Set integer catalog property */
    default void setCatalogProperty(String catalogAlias, String key, int value)
    {
        setCatalogProperty(catalogAlias, key, ValueVector.literalInt(value, 1));
    }

    /** Set boolean catalog property */
    default void setCatalogProperty(String catalogAlias, String key, boolean value)
    {
        setCatalogProperty(catalogAlias, key, ValueVector.literalBoolean(value, 1));
    }

    /** Set object catalog property */
    default void setCatalogProperty(String catalogAlias, String key, Object value)
    {
        setCatalogProperty(catalogAlias, key, value == null ? ValueVector.literalNull(ResolvedType.of(Type.Any), 1)
                : ValueVector.literalAny(1, value));
    }

    /** Get the generic cache */
    GenericCache getGenericCache();

    /** Returns true if the query should be aborted */
    boolean abortQuery();

    /**
     * Register an abort query listener to session. This is called if the query should be aborted and {@link IDatasource}'s can register to close connections etc. NOTE! Are called on a separate thread
     * from the thread that executes the query
     */
    void registerAbortListener(Runnable listener);

    /** Unregister provided abort listener */
    void unregisterAbortListener(Runnable listener);

    /** Get the print writer used for message outputs */
    Writer getPrintWriter();

    /**
     * Handle known exception from catalogs. For example a SQLException that is thrown during execution but is not crucial for the whole execution.
     */
    default void handleKnownException(Exception e)
    {
        try
        {
            getPrintWriter().append(e.getMessage())
                    .append(System.lineSeparator());
        }
        catch (IOException e1)
        {
            // Swallow this
        }
    }

    /** Return the execution time in ms. for the last executed statement */
    long getLastQueryExecutionTime();

    /** Return the row count for the last executed statement */
    long getLastQueryRowCount();
}
