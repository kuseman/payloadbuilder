package org.kuse.payloadbuilder.catalog.jdbc;

import java.awt.Component;
import java.util.Map;

/**
 * Definition of a connection provider.
 *
 * <pre>
 * Used to build specific jdbc urls etc.
 * Provides connection specific UI for configuration and properties
 * </pre>
 **/
interface ConnectionProvider
{
    /** Returns component used when adding/editing a connection for this provider */
    Component getComponent();

    /** Init component from properties */
    void initComponent(Map<String, Object> properties);

    /** Construct jdbc url from properties */
    String getURL(Map<String, Object> properties);

    /** Return driver class name for this provider */
    String getDriverClassName();
}
