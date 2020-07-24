package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.HashMap;
import java.util.Map;

import org.kuse.payloadbuilder.core.catalog.builtin.BuiltinCatalog;

/** Catalog registry */
public class CatalogRegistry
{
    public static final String DEFAULTCATALOG = "defaultCatalog";
    private final Map<String, Catalog> catalogByAlias = new HashMap<>();
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
        catalogByAlias.put(lowerCase(alias), catalog);
    }

    /** Get catalog */
    public Catalog getCatalog(String alias)
    {
        return isBlank(alias) ? builtinCatalog : catalogByAlias.get(alias);
    }
    
    /** Clears registered catalogs */
    public void clearCatalogs()
    {
        catalogByAlias.clear();
    }
}
