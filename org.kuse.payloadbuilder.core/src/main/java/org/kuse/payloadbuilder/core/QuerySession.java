package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BooleanSupplier;

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.Tuple;
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
    private PrintStream printStream;
    private BooleanSupplier abortSupplier;
    private CacheProvider cacheProvider; /* = new MapCacheProvider();*/

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

    public CacheProvider getCacheProvider()
    {
        return cacheProvider;
    }

    public void setCacheProvider(CacheProvider cacheProvider)
    {
        this.cacheProvider = cacheProvider;
    }

    /** Test provider */
    private class MapCacheProvider implements CacheProvider
    {
        private final Map<Object, List<Tuple>> cache = new HashMap<>();

        @Override
        public <TKey> Map<TKey, List<Tuple>> getAll(Iterable<TKey> keys)
        {
            Map<TKey, List<Tuple>> result = new HashMap<>();
            for (TKey key  : keys)
            {
                Object cacheKey = key;
                if (cacheKey instanceof CacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }

                result.put(key, cache.get(cacheKey));
            }
            return result;
        }

        @Override
        public <TKey> void putAll(Map<TKey, List<Tuple>> values, Duration ttl)
        {
            for (Entry<TKey, List<Tuple>> entry : values.entrySet())
            {
                Object cacheKey = entry.getKey();
                if (cacheKey instanceof CacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }
                cache.put(cacheKey, entry.getValue());
            }
        }
    }
}
