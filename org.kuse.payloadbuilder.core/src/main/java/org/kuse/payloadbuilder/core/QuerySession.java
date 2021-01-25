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
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.VariableExpression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

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
    private TupleCacheProvider tupleCacheProvider = new MapCacheProvider();

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

    public TupleCacheProvider getTupleCacheProvider()
    {
        return tupleCacheProvider;
    }

    public void setTupleCacheProvider(TupleCacheProvider cacheProvider)
    {
        this.tupleCacheProvider = cacheProvider;
    }

    /** Test provider */
    private class MapCacheProvider implements TupleCacheProvider
    {
        private final Map<String, Map<Object, List<Tuple>>> cache = new HashMap<>();

        @Override
        public <TKey> Map<TKey, List<Tuple>> getAll(String cacheName, Iterable<TKey> keys)
        {
            Map<TKey, List<Tuple>> result = new HashMap<>();
            for (TKey key : keys)
            {
                Object cacheKey = key;
                if (cacheKey instanceof TupleCacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }

                result.put(key, cache.computeIfAbsent(cacheName, k -> new HashMap<>()).get(cacheKey));
            }
            return result;
        }

        @Override
        public <TKey> void putAll(String cacheName, Map<TKey, List<Tuple>> values, Duration ttl)
        {
            for (Entry<TKey, List<Tuple>> entry : values.entrySet())
            {
                Object cacheKey = entry.getKey();
                if (cacheKey instanceof TupleCacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }
                cache.computeIfAbsent(cacheName, k -> new HashMap<>()).put(cacheKey, entry.getValue());
            }
        }

        @Override
        public Map<String, Map<String, Object>> describe()
        {
            return cache.entrySet().stream().map(e ->
            {
                return Pair.of(e.getKey(), MapUtils.ofEntries(MapUtils.entry("size", (Object) e.getValue().size())));
            })
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }

        @Override
        public void removeCache(String cacheName)
        {
            cache.remove(cacheName);
        }

        @Override
        public void flushCache(String cacheName)
        {
            Map<Object, List<Tuple>> c = cache.get(cacheName);
            if (c != null)
            {
                c.clear();
            }
        }

        @Override
        public void flushCache(String cacheName, Object key)
        {
            Map<Object, List<Tuple>> c = cache.get(cacheName);
            if (c != null)
            {
                c.remove(key);
            }
        }
    }
}
