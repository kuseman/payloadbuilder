/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
