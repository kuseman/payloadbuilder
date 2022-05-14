package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.cache.BatchCache;

/**
 * <pre>
 * Cache operator used in conjunction with BatchXXXX operators.
 * Enables caching of inner operator rows.
 *
 * ie.
 *
 * select *
 * from tableA a
 * inner join tableB b with (cacheName = 'tableB', cacheKey = @param , cacheTTL = 10m)
 *   on b.id = a.id
 *   and b.value = @param
 *
 * If the join satisfies an indexed batch join the cache will be applied
 * and will sit between the inner operator and the batch join
 *
 * BatchXXXXJoin
 *   scan (tableA)
 *   BatchCacheOperator
 *     index (tableB)
 *
 * No caching of filters
 *
 * BatchXXXXJoin
 *   scan (tableA)
 *   Filter
 *     (BatchCacheOperator)
 *       index (tableB)
 *
 * Caching of filters
 *
 * BatchXXXXJoin
 *   scan (tableA)
 *   (BatchCacheOperator)
 *     Filter
 *       index (tableB)
 * </pre>
 */
class BatchCacheOperator extends AOperator
{
    private final Operator operator;
    /** Factory that create {@link IOrdinalValues} for the inner tuples */
    private final IOrdinalValuesFactory innerOrdinalValuesFactory;
    private final CacheSettings settings;

    BatchCacheOperator(int nodeId, Operator operator, IOrdinalValuesFactory innerOrdinalValuesFactory, CacheSettings settings)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.innerOrdinalValuesFactory = requireNonNull(innerOrdinalValuesFactory, "innerOrdinalValuesFactory");
        this.settings = requireNonNull(settings, "settings");
    }

    @Override
    public String getName()
    {
        return "Batch cache";
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = ofEntries(entry("Cache expression", String.valueOf(settings.cacheKeyExpression)), entry("TTL", settings.ttlExpression.toString()));

        return properties;
    }

    @Override
    public TupleIterator open(IExecutionContext ctx)
    {
        ExecutionContext context = (ExecutionContext) ctx;
        Duration ttl = null;
        Object obj = settings.ttlExpression.apply(context);
        if (obj != null)
        {
            ttl = Duration.parse(String.valueOf(obj));
        }

        obj = settings.nameExpression.apply(context);
        if (obj == null)
        {
            throw new OperatorException("Cache name evaluated to null");
        }

        boolean readOnly = BooleanUtils.isTrue((Boolean) context.getSession()
                .getSystemProperty(QuerySession.BATCH_CACHE_READ_ONLY));

        context.getStatementContext()
                .setTuple(null);
        Object cacheKey = settings.cacheKeyExpression != null ? settings.cacheKeyExpression.apply(context)
                : null;

        BatchCache cache = context.getSession()
                .getBatchCache();
        QualifiedName cacheName = QualifiedName.of(obj);
        Map<CacheKey, List<Tuple>> cachedValues = cache.getAll(cacheName, getCacheKeyIterable(context, cacheKey));
        TupleIterator it2 = getAndCache(context, readOnly, cache, cacheName, cacheKey, ttl, cachedValues);

        final Iterator<Entry<CacheKey, List<Tuple>>> it1 = cachedValues.entrySet()
                .iterator();
        // CSOFF
        // Return a row iterator that first iterates cached values and then non cached ones
        return new TupleIterator()
        // CSON
        {
            private boolean cachedIterator = true;
            private List<Tuple> currentList;
            private int index;
            private Tuple next;

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (cachedIterator)
                    {
                        currentList = setNextList();
                        if (currentList == null)
                        {
                            cachedIterator = false;
                        }
                        else if (index >= currentList.size())
                        {
                            currentList = null;
                            index = 0;
                        }
                        else
                        {
                            next = currentList.get(index);
                            index++;
                        }
                    }
                    else
                    {
                        if (!it2.hasNext())
                        {
                            return false;
                        }

                        next = it2.next();
                    }
                }

                return true;
            }

            private List<Tuple> setNextList()
            {
                if (currentList != null)
                {
                    return currentList;
                }
                else if (!it1.hasNext())
                {
                    return null;
                }

                List<Tuple> list = null;
                while (list == null
                        && it1.hasNext())
                {
                    list = it1.next()
                            .getValue();
                }

                return list;
            }
        };
    }

    /** Get and cache values from down stream operator */
    private TupleIterator getAndCache(ExecutionContext context, boolean readOnly, BatchCache cache, QualifiedName cacheName, Object cacheKey, Duration ttl,
            Map<CacheKey, List<Tuple>> cachedValues)
    {
        // Put a non cached outer values iterator to context
        Iterator<IOrdinalValues> outerValuesIterator = getOuterValuesIterator(cachedValues);
        // No values to fetch, everything was present in cache
        if (!outerValuesIterator.hasNext())
        {
            return TupleIterator.EMPTY;
        }
        context.getStatementContext()
                .setOuterOrdinalValues(outerValuesIterator);

        TupleIterator it = operator.open(context);

        if (readOnly)
        {
            return it;
        }

        Map<CacheKey, List<Tuple>> values = getInnerTuples(context, it, cacheKey);

        // Add missing new keys to cache as empty lists
        for (Entry<CacheKey, List<Tuple>> e : cachedValues.entrySet())
        {
            // Non cached value, make sure it's getting cached with an empty list
            if (e.getValue() == null)
            {
                values.putIfAbsent(e.getKey(), emptyList());
            }
        }

        // Trim arrays before put to cache
        for (Entry<CacheKey, List<Tuple>> e : values.entrySet())
        {
            if (e.getValue() instanceof ArrayList)
            {
                ((ArrayList<Tuple>) e.getValue()).trimToSize();
            }
        }

        cache.putAll(cacheName, values, ttl);

        final Iterator<Entry<CacheKey, List<Tuple>>> valuesIt = values.entrySet()
                .iterator();
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            Tuple next;
            List<Tuple> current;
            int index;

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (current == null)
                    {
                        if (!valuesIt.hasNext())
                        {
                            return false;
                        }

                        while (current == null
                                && valuesIt.hasNext())
                        {
                            current = valuesIt.next()
                                    .getValue();
                            if (current.isEmpty())
                            {
                                current = null;
                            }
                        }
                        index = 0;
                    }
                    else if (index >= current.size())
                    {
                        current = null;
                    }
                    else
                    {
                        next = current.get(index++);
                    }
                }
                return true;
            }
        };
    }

    private Map<CacheKey, List<Tuple>> getInnerTuples(ExecutionContext context, TupleIterator it, Object cacheKey)
    {
        Map<CacheKey, List<Tuple>> values = new LinkedHashMap<>();
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            context.getStatementContext()
                    .setTuple(tuple);

            CacheKey key = new CacheKey(cacheKey, innerOrdinalValuesFactory.create(context, tuple));
            // Optimize the tuple before put to cache
            values.compute(key, (k, v) ->
            {
                if (v == null)
                {
                    return singletonList(tuple);
                }
                else if (v.size() == 1)
                {
                    List<Tuple> list = new ArrayList<>(2);
                    list.add(v.get(0));
                    list.add(tuple);
                    return list;
                }

                v.add(tuple);
                return v;
            });
        }
        it.close();
        return values;
    }

    /**
     * Get an outer values iterator based on the cached values that will be "sent" to down stream operator Ie. Return outer values for entries that we don't found a cached value for
     */
    private Iterator<IOrdinalValues> getOuterValuesIterator(final Map<CacheKey, List<Tuple>> cachedValues)
    {
        final Iterator<Entry<CacheKey, List<Tuple>>> iterator = cachedValues.entrySet()
                .iterator();
        // CSOFF
        return new Iterator<IOrdinalValues>()
        // CSON
        {
            private IOrdinalValues next;

            @Override
            public IOrdinalValues next()
            {
                IOrdinalValues result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!iterator.hasNext())
                    {
                        return false;
                    }

                    Entry<CacheKey, List<Tuple>> entry = iterator.next();
                    // Cached data present, move on to next
                    if (entry.getValue() != null)
                    {
                        continue;
                    }
                    next = entry.getKey().values;
                }
                return true;
            }
        };
    }

    /**
     * Create an iterable that transforms outer key values from context to a stream of cache keys
     */
    private Iterable<CacheKey> getCacheKeyIterable(ExecutionContext context, Object cacheKey)
    {
        // CSOFF
        return () -> new Iterator<CacheKey>()
        // CSON
        {
            @Override
            public CacheKey next()
            {
                IOrdinalValues ordinalValues = context.getStatementContext()
                        .getOuterOrdinalValues()
                        .next();
                return new CacheKey(cacheKey, ordinalValues);
            }

            @Override
            public boolean hasNext()
            {
                return context.getStatementContext()
                        .getOuterOrdinalValues()
                        .hasNext();
            }
        };
    }

    /** Settings for cache */
    static class CacheSettings
    {
        /** Cache name */
        private final Function<ExecutionContext, Object> nameExpression;
        /** Optional cache key that is added besides the outer/inner values. Should be a constant value */
        private final Function<ExecutionContext, Object> cacheKeyExpression;
        /** TTL expression in https://en.wikipedia.org/wiki/ISO_8601#Durations format */
        private final Function<ExecutionContext, Object> ttlExpression;

        CacheSettings(Function<ExecutionContext, Object> nameExpression, Function<ExecutionContext, Object> cacheKeyExpression, Function<ExecutionContext, Object> ttlExpression)
        {
            this.nameExpression = requireNonNull(nameExpression, "nameExpression");
            this.cacheKeyExpression = cacheKeyExpression;
            this.ttlExpression = ttlExpression;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nameExpression, cacheKeyExpression);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof CacheSettings)
            {
                CacheSettings that = (CacheSettings) obj;
                return nameExpression.equals(that.nameExpression)
                        && Objects.equals(cacheKeyExpression, that.cacheKeyExpression)
                        && Objects.equals(ttlExpression, that.ttlExpression);
            }
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof BatchCacheOperator)
        {
            BatchCacheOperator that = (BatchCacheOperator) obj;
            return nodeId == that.nodeId
                    && operator.equals(that.operator)
                    && settings.equals(that.settings);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("OUTER VALUES CACHE (ID: %d, TTL: %s, CACHE KEY: %s)", nodeId, settings.ttlExpression.toString(), String.valueOf(settings.cacheKeyExpression)) + System.lineSeparator()
                + indentString + operator.toString(indent + 1);
    }

    /** Cache key */
    static class CacheKey
    {
        private final Object cacheKey;
        private final IOrdinalValues values;

        CacheKey(Object cacheKey, IOrdinalValues values)
        {
            this.cacheKey = cacheKey;
            this.values = values;
        }

        @Override
        public int hashCode()
        {
            return values.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof CacheKey)
            {
                CacheKey that = (CacheKey) obj;
                return Objects.equals(cacheKey, that.cacheKey)
                        && values.equals(that.values);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return (cacheKey != null ? cacheKey.toString()
                    : "")
                   + " "
                   + ArrayUtils.toString(values);
        }
    }
}
