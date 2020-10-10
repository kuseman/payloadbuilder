package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.builtin.BuiltinCatalog;

/** Catalog registry */
public class CatalogRegistry
{
    public static final String DEFAULTCATALOG = "defaultCatalog";
    private final Map<String, Catalog> catalogByAlias = new HashMap<>();
    private final Catalog builtinCatalog;
    private String defaultCatalogAlias;

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

    /** Get default catalog for this session */
    public Catalog getDefaultCatalog()
    {
        return isBlank(defaultCatalogAlias) ? null : getCatalog(defaultCatalogAlias);
    }

    /** Return default catalog alias */
    public String getDefaultCatalogAlias()
    {
        return defaultCatalogAlias;
    }

    /**
     * Get default catalog for this session
     *
     * @param alias Alias of the catalog to set as default
     **/
    public void setDefaultCatalog(String alias)
    {
        defaultCatalogAlias = alias;
    }

    /**
     * Resolves function info from provided
     */
    public Pair<String, FunctionInfo> resolveFunctionInfo(String catalogAlias, String function)
    {
        Catalog catalog;
        if (isBlank(catalogAlias))
        {
            // First try default catalog
            catalog = getDefaultCatalog();
            if (catalog != null)
            {
                FunctionInfo f = catalog.getFunction(lowerCase(function));
                //CSOFF
                if (f != null)
                //CSON
                {
                    return Pair.of(defaultCatalogAlias, f);
                }
            }

            catalog = getBuiltin();
        }
        else
        {
            catalog = getCatalog(lowerCase(catalogAlias));
        }

        if (catalog == null)
        {
            return null;
        }

        FunctionInfo f = catalog.getFunction(lowerCase(function));
        if (f == null)
        {
            return null;
        }
        return Pair.of(catalogAlias, f);
    }
}
