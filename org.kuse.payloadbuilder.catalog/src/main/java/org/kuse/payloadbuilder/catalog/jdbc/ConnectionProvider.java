package org.kuse.payloadbuilder.catalog.jdbc;

import java.awt.Component;
import java.util.Map;

/**
 * Definition of a connection provider. Used to build specific connection strings etc. Provider specific UI extension
 **/
interface ConnectionProvider
{
    /** Returns component used when adding/editing a connection for this provider */
    Component getComponent();

    /** Init component from properties */
    void initComponent(Map<String, Object> properties);

    /** Construct connection string from properties */
    String getConnectionString(Map<String, Object> properties);
}
