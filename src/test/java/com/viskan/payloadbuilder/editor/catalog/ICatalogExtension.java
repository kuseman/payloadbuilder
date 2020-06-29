package com.viskan.payloadbuilder.editor.catalog;

import java.beans.PropertyChangeListener;

/** Definition of a catalog extension */
public interface ICatalogExtension
{
    /** Get title of extension */
    String getTitle();
    
    /** Get default alias of catalog */
    String getDefaultAlias();
    
    /** Add property change listener */
    void addPropertyChangeListener(PropertyChangeListener listener);
    /** Remove property change listener */
    void removePropertyChangeListener(PropertyChangeListener listener);
}
