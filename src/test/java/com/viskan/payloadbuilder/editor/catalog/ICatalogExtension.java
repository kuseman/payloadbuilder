package com.viskan.payloadbuilder.editor.catalog;

import com.viskan.payloadbuilder.QuerySession;

import static java.util.Collections.emptyMap;

import java.awt.Component;
import java.util.Map;

/** Definition of a catalog extension */
public interface ICatalogExtension
{
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
     * */
    void setup(String catalogAlias, QuerySession querySession);
    
    /** Update the extension based on the query session. Ie
     * acting upon changed variables etc. 
     * @param catalogAlias Alias that this extension/catalog has been given
     * */
    void update(String catalogAlias, QuerySession querySession);
    
    /** Get configuration component. Will be showed in a dialog
     * for setting up the extension */
    default Component getConfigComponent()
    {
        return null;
    }
    
    /** Get quick properties component. Will be showed in extensions side bar
     * with quick properties. Ie. selected. database/index. */
    Component getQuickPropertiesComponent();
    
//    /** Add property change listener */
//    void addPropertyChangeListener(PropertyChangeListener listener);
//    /** Remove property change listener */
//    void removePropertyChangeListener(PropertyChangeListener listener);
}
