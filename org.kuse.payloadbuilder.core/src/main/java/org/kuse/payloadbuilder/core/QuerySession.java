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
package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.parser.VariableExpression;

/**
 * A query session. Holds properties for catalog implementations etc. Can live through multiple query executions
 **/
public class QuerySession
{
    private final CatalogRegistry catalogRegistry;
    /** Variable values for {@link VariableExpression}'s */
    private final Map<String, Object> variables;
    /** Catalog properties by catalog alias */
    private Map<String, Map<String, Object>> catalogProperties;

    private String defaultCatalogAlias;
    private PrintStream printStream;
    private BooleanSupplier abortSupplier;

    public QuerySession(CatalogRegistry catalogRegistry)
    {
        this(catalogRegistry, emptyMap());
    }

    public QuerySession(CatalogRegistry catalogRegistry, Map<String, Object> variables)
    {
        this.catalogRegistry = requireNonNull(catalogRegistry, "catalogRegistry");
        this.variables = requireNonNull(variables, "variables");
    }

    /** Set print stream */
    public void setPrintStream(PrintStream printStream)
    {
        this.printStream = printStream;
    }

    /** Set abort supplier. */
    public void setAbortSupplier(BooleanSupplier abortSupplier)
    {
        this.abortSupplier = abortSupplier;
    }

    /** Abort current query */
    public boolean abortQuery()
    {
        return abortSupplier != null && abortSupplier.getAsBoolean();
    }

    /** Return catalog registry */
    public CatalogRegistry getCatalogRegistry()
    {
        return catalogRegistry;
    }

    /** Get default catalog for this session */
    public Catalog getDefaultCatalog()
    {
        return isBlank(defaultCatalogAlias) ? null : catalogRegistry.getCatalog(defaultCatalogAlias);
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

    /** Return variables map */
    public Map<String, Object> getVariables()
    {
        return variables;
    }

    /** Print value to print stream if set */
    public void printLine(Object value)
    {
        if (printStream != null)
        {
            printStream.println(value);
        }
    }

    /** Set catalog property */
    public void setCatalogProperty(String alias, String key, Object value)
    {
        if (catalogProperties == null)
        {
            catalogProperties = new HashMap<>();
        }

        catalogProperties
                .computeIfAbsent(alias, k -> new HashMap<>())
                .put(key, value);
    }

    /** Get catalog property */
    public Object getCatalogProperty(String alias, String key)
    {
        if (catalogProperties == null)
        {
            return null;
        }

        return catalogProperties.getOrDefault(alias, emptyMap()).get(key);
    }

    /**
     * Resolves function info from provided
     *
     * @param functionId Unique function id to cache a lookup
     */
    public FunctionInfo resolveFunctionInfo(String catalogAlias, String function, int functionId)
    {
        Catalog catalog;
        if (catalogAlias == null)
        {
            catalog = catalogRegistry.getBuiltin();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(catalogAlias);
        }

        if (catalog == null)
        {
            return null;
        }

        return catalog.getFunction(function);
    }
}
