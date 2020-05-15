package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.catalog.builtin.BuiltinCatalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.HashMap;
import java.util.Map;

/** Catalog registry  */
public class CatalogRegistry
{
    private final Map<String, Catalog> catalogByName = new HashMap<>();
    private final Catalog builtinCatalog;
    private Catalog defaultCatalog;
    
    public CatalogRegistry()
    {
        // Register default catalog
        BuiltinCatalog.register(this);
        builtinCatalog = getCatalog(BuiltinCatalog.NAME);
    }
    
    public Catalog getBuiltin()
    {
        return builtinCatalog;
    }
    
    public Catalog getDefaultCatalog()
    {
        return defaultCatalog;
    }
    
    public void setDefaultCatalog(Catalog defaultCatalog)
    {
        this.defaultCatalog = defaultCatalog;
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
        return catalogByName.get(isBlank(name) ? BuiltinCatalog.NAME : name);
    }
    
    // TODO: handle table resolving. Default catalog would be different for tables
    //       to avoid needing to write "CATALOG.tablename" everywhere
    //       Will be inconsistent with scalar function that would resolve to default/builtin
    //       CatalogResolver ? Defaults to built-in for functions and throws for tables
}
