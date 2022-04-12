package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;

/** Catalog registry */
public class CatalogRegistry
{
    private final Map<String, Catalog> catalogByAlias = new LinkedHashMap<>();
    private final Catalog systemCatalog = SystemCatalog.get();

    public CatalogRegistry()
    {
    }

    public Catalog getSystemCatalog()
    {
        return systemCatalog;
    }

    /** Register catalog */
    public void registerCatalog(String alias, Catalog catalog)
    {
        if (SystemCatalog.ALIAS.equals(lowerCase(alias)))
        {
            throw new IllegalArgumentException("Cannot register a catalog with reserved alias 'sys'");
        }

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
        if (SystemCatalog.ALIAS.equals(lowerCase(alias)))
        {
            return systemCatalog;
        }
        return isBlank(alias) ? systemCatalog
                : catalogByAlias.get(alias);
    }

    /** Return set of registered catalogs */
    public Set<Entry<String, Catalog>> getCatalogs()
    {
        return catalogByAlias.entrySet();
    }
}
