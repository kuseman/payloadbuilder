package org.kuse.payloadbuilder.editor;

import static java.util.Collections.emptyMap;

import java.awt.Component;
import java.beans.PropertyChangeListener;
import java.util.Map;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogException;

/** Definition of a catalog extension */
public interface ICatalogExtension
{
    /** Property changed keys */
    String PROPERTIES = "properties";

    /** Get title of extension */
    String getTitle();

    /** Get default alias of catalog */
    String getDefaultAlias();

    /**
     * Get properties for this extension. Is the properties that is stored/loaded ie. config/settings.
     */
    default Map<String, Object> getProperties()
    {
        return emptyMap();
    }

    /**
     * Load the extension with provided properties
     *
     * @param properties Propeties to load into extension
     */
    default void load(Map<String, Object> properties)
    {
    }

    /**
     * Setup session before query is executed. Ie. set selected database/index etc.
     *
     * @param catalogAlias Alias that this extension/catalog has been given
     * @param querySession Current query session
     **/
    default void setup(String catalogAlias, QuerySession querySession)
    {
    }

    /**
     * Update the extension based on the query session. Ie acting upon changed variables etc.
     *
     * @param catalogAlias Alias that this extension/catalog has been given
     * @param querySession Current query session
     **/
    default void update(String catalogAlias, QuerySession querySession)
    {
    }

    /**
     * Get configuration component. Will be showed in a dialog for setting up the extension
     */
    default Component getConfigComponent()
    {
        return null;
    }

    /**
     * Get quick properties component. Will be showed in extensions side bar with quick properties. Ie. selected. database/index.
     */
    default Component getQuickPropertiesComponent()
    {
        return null;
    }

    /** Get the actual catalog implementation for this extension */
    Catalog getCatalog();

    /** Handle provided exception
     * @param querySession Current query session
     * @param exception Exception to handle
     **/
    default ExceptionAction handleException(QuerySession querySession, CatalogException exception)
    {
        return ExceptionAction.NONE;
    }

    /**
     * Add property change listener
     *
     * @param listener Listener to add
     **/
    default void addPropertyChangeListener(PropertyChangeListener listener)
    {
    }

    /**
     * Remove property change listener
     *
     * @param listener Listener to remove
     **/
    default void removePropertyChangeListener(PropertyChangeListener listener)
    {
    }

    /** Action that should be performed after handling of an Exception */
    enum ExceptionAction
    {
        /** Do nothing action */
        NONE,
        /** Re-run query. */
        RERUN;
    }
}
