package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.catalog.builtin.BuiltinCatalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.HashMap;
import java.util.Map;

/** Catalog registry */
public class CatalogRegistry
{
    public static final String DEFAULTCATALOG = "defaultCatalog";
    private final Map<String, Catalog> catalogByName = new HashMap<>();
    private final Catalog builtinCatalog;

    public CatalogRegistry()
    {
        builtinCatalog = BuiltinCatalog.get();
    }

    public Catalog getBuiltin()
    {
        return builtinCatalog;
    }

    /** Register catalog */
    public void registerCatalog(String alias, Catalog catalog)
    {
        requireNonNull(catalog, "catalog");
        if (isBlank(alias))
        {
            throw new IllegalArgumentException("Alias cannot be blank");
        }
        catalogByName.put(alias, catalog);
    }

    /** Get catalog */
    public Catalog getCatalog(String name)
    {
        return isBlank(name) ? builtinCatalog : catalogByName.get(name);
    }
}
