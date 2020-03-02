package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.catalog._default.DefaultCatalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.HashMap;
import java.util.Map;

/** Catalog registry  */
public class CatalogRegistry
{
    private final Map<String, Catalog> catalogByName = new HashMap<>();
    private final Catalog defaultCatalog;
    
    public CatalogRegistry()
    {
        // Register default catalog
        DefaultCatalog.register(this);
        defaultCatalog = getCatalog(DefaultCatalog.NAME);
    }
    
    public Catalog getDefault()
    {
        return defaultCatalog;
    }
    
    /** Register catalog */
    public void registerCatalog(Catalog catalog)
    {
        requireNonNull(catalog, "catalog");
        catalogByName.put(catalog.getName(), catalog);
    }
    
    /** Get catalog */
    public Catalog getCatalog(String name)
    {
        return catalogByName.get(isBlank(name) ? DefaultCatalog.NAME : name);
    }
}
