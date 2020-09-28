package org.kuse.payloadbuilder.editor;

import static java.util.Collections.emptyMap;

import java.awt.Component;
import java.beans.PropertyChangeListener;
import java.util.Map;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;

/** Definition of a catalog extension */
public interface ICatalogExtension
{
    /** Property changed keys */
    static final String PROPERTIES = "properties";
    static final String CONFIG = "config";
    
    /** Get title of extension */
    String getTitle();
    
    /** Get default alias of catalog */
    String getDefaultAlias();
    
    /** Get properties for this extension. Is the
     * properties that is stored/loaded ie. config/settings. */
    default Map<String, Object> getProperties()
    {
        return emptyMap();
    }
    
    /** Load the extension with provided properties
     * @param properties Propeties to load into extension */
    default void load(Map<String, Object> properties)
    {
    }
    
    /** Setup session before query is executed. Ie. set selected 
     * database/index etc. 
     * @param catalogAlias Alias that this extension/catalog has been given
     * @param querySession Current query session
     **/
    default void setup(String catalogAlias, QuerySession querySession)
    {
    }
    
    /** Update the extension based on the query session. Ie
     * acting upon changed variables etc. 
     * @param catalogAlias Alias that this extension/catalog has been given
     * @param querySession Current query session
     **/
    default void update(String catalogAlias, QuerySession querySession)
    {
    }
    
    /** Get configuration component. Will be showed in a dialog
     * for setting up the extension */
    default Component getConfigComponent()
    {
        return null;
    }
    
    /** Get quick properties component. Will be showed in extensions side bar
     * with quick properties. Ie. selected. database/index. */
    default Component getQuickPropertiesComponent()
    {
        return null;
    }

    /** Get the actual catalog implementation for this extension */
    Catalog getCatalog();
    
    /** Add property change listener
     * @param listener Listener to add
     **/
    default void addPropertyChangeListener(PropertyChangeListener listener)
    {}
    
    /** Remove property change listener 
     * @param listener Listener to remove
     **/
    default void removePropertyChangeListener(PropertyChangeListener listener)
    {}
}
