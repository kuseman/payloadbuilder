package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.TupleCacheProvider;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.operator.OperatorContext.OuterValues;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * <pre>
 * Cache operator that caches tuples with a
 * probing outer key expression and put values to cache
 * with an inner key expression.
 *
 * NOTE! If operatorContext has an {@link OperatorContext#getOuterIndexValues()) set
 * that one is consumed by outer keys expression as lookup in cache.
 *
 * Else outer key is evaluated once and assumed constant for lookup in cache.
 * </pre>
 */
class OuterValuesCacheOperator extends AOperator
{
    private final Operator operator;
    private final Catalog operatorCatalog;
    private final String operatorCatalogAlias;
    /** Cache name */
    private final Expression nameExpression;
    /** Cache key for the inner relation */
    private final Expression outerKeyExpression;
    /** Cache key for the outer relation */
    private final Expression innerKeyExpression;
    /** TTL expression in https://en.wikipedia.org/wiki/ISO_8601#Durations format */
    private final Expression ttlExpression;

    OuterValuesCacheOperator(
            int nodeId,
            Operator operator,
            Catalog operatorCatalog,
            String operatorCatalogAlias,
            Expression nameExpression,
            Expression outerKeyExpression,
            Expression innerKeyExpression,
            Expression ttlExpression)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.operatorCatalog = requireNonNull(operatorCatalog, "operatorCatalog");
        this.operatorCatalogAlias = requireNonNull(operatorCatalogAlias, "operatorCatalogAlias");
        this.nameExpression = requireNonNull(nameExpression, "nameExpression");
        this.outerKeyExpression = requireNonNull(outerKeyExpression, "outerKeyExpression");
        this.innerKeyExpression = requireNonNull(innerKeyExpression, "innerKeyExpression");
        this.ttlExpression = requireNonNull(ttlExpression, "ttlExpression");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return singletonList(operator);
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(
                entry("Outer key", outerKeyExpression.toString()),
                entry("Inner key", innerKeyExpression.toString()),
                entry("TTL", ttlExpression.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        TupleCacheProvider cacheProvider = context.getSession().getTupleCacheProvider();
        if (cacheProvider == null)
        {
            throw new OperatorException("Cannot use cache table option without a registered CacheProvider in session");
        }

        Duration ttl = null;
        Object obj = ttlExpression.eval(context);
        if (obj != null)
        {
            ttl = Duration.parse(String.valueOf(obj));
        }

        obj = nameExpression.eval(context);
        if (obj == null)
        {
            throw new OperatorException("Cache name evaluated to null");
        }

        String name = String.valueOf(obj);
        String cacheName = operatorCatalog.prepareTupleCacheName(context, operatorCatalogAlias, name);
        boolean hasOuterValues = context.getOperatorContext().getOuterIndexValues() != null;
        Map<CacheKeyImpl, List<Tuple>> cachedValues = cacheProvider.getAll(cacheName, getCacheKeyIterable(context, hasOuterValues));
        Map<Object, List<Tuple>> nonCachedValues = getAndCache(context, hasOuterValues, cacheProvider, cacheName, ttl, cachedValues);

        final Iterator<Entry<CacheKeyImpl, List<Tuple>>> it1 = cachedValues.entrySet().iterator();
        final Iterator<Entry<Object, List<Tuple>>> it2 = nonCachedValues.size() > 0
            ? nonCachedValues.entrySet().iterator()
            : emptyIterator();

        //CSOFF
        // Return a row iterator that first iterates cached values and then non cached ones
        return new RowIterator()
        //CSON
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
                while (next == null)
                {
                    if (currentList == null)
                    {
                        if (!setNextList())
                        {
                            return false;
                        }
                    }
                    else if (index >= currentList.size())
                    {
                        currentList = null;
                    }
                    else
                    {
                        next = currentList.get(index);
                        index++;
                    }
                }

                return true;
            }

            private boolean setNextList()
            {
                if (cachedIterator)
                {
                    if (!it1.hasNext())
                    {
                        cachedIterator = false;
                        return true;
                    }

                    currentList = it1.next().getValue();
                }
                else
                {
                    if (!it2.hasNext())
                    {
                        // done
                        return false;
                    }
                    currentList = it2.next().getValue();
                }
                index = 0;
                return true;
            }
        };
    }

    /** Get and cache values from down stream operator */
    private Map<Object, List<Tuple>> getAndCache(
            ExecutionContext context,
            boolean hasOuterValues,
            TupleCacheProvider cacheProvider,
            String name,
            Duration ttl,
            Map<CacheKeyImpl, List<Tuple>> cachedValues)
    {
        // Put a non cached values outer values iterator to context
        if (hasOuterValues)
        {
            Iterator<OuterValues> outerValuesIterator = getOuterValuesIterator(cachedValues);
            // No values to fetch, everything was present in cache
            if (!outerValuesIterator.hasNext())
            {
                return emptyMap();
            }
            context.getOperatorContext().setOuterIndexValues(outerValuesIterator);
        }
        // No outer values mode and there are cached values then no need to open down stream
        else if (cachedValues.entrySet().stream().anyMatch(e -> e.getValue() != null && e.getValue().size() > 0))
        {
            return emptyMap();
        }

        RowIterator it = operator.open(context);

        Map<Object, List<Tuple>> values = new LinkedHashMap<>();
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            context.setTuple(tuple);
            final Object key = innerKeyExpression.eval(context);
            values.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }
        it.close();

        // Add missing new keys to cache as empty lists
        for (Entry<CacheKeyImpl, List<Tuple>> e : cachedValues.entrySet())
        {
            values.putIfAbsent(e.getKey().cacheKey, emptyList());
        }
        cacheProvider.putAll(name, values, ttl);
        return values;
    }

    /**
     * Get an outer values iterator based on the cached values that will be "sent" to down stream operator Ie. Return outer values for entries that we
     * don't found a cached value for
     */
    private Iterator<OuterValues> getOuterValuesIterator(final Map<CacheKeyImpl, List<Tuple>> cachedValues)
    {
        OuterValues outerValues = new OuterValues();
        //CSOFF
        return new Iterator<OperatorContext.OuterValues>()
        //CSON
        {
            private final Iterator<Entry<CacheKeyImpl, List<Tuple>>> iterator = cachedValues.entrySet().iterator();
            private CacheKeyImpl cacheKey;

            @Override
            public OuterValues next()
            {
                outerValues.setOuterTuple(cacheKey.outerTuple);
                outerValues.setValues(cacheKey.outerValues);
                cacheKey = null;
                return outerValues;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (cacheKey == null)
                {
                    if (!iterator.hasNext())
                    {
                        return false;
                    }

                    Entry<CacheKeyImpl, List<Tuple>> entry = iterator.next();
                    // Cached data present, move on to next
                    if (entry.getValue() != null)
                    {
                        continue;
                    }

                    cacheKey = entry.getKey();
                }
                return true;
            }
        };
    }

    /**
     * Create an iterable that transforms outer key values from context to a stream of cache keys
     *
     * @param hasOuterValues
     */
    @SuppressWarnings("unchecked")
    private Iterable<CacheKeyImpl> getCacheKeyIterable(ExecutionContext context, boolean hasOuterValues)
    {
        if (!hasOuterValues)
        {
            return () -> new SingletonIterator(new CacheKeyImpl(outerKeyExpression.eval(context), null, null));
        }

        //CSOFF
        return () -> new Iterator<CacheKeyImpl>()
        //CSON
        {
            @Override
            public CacheKeyImpl next()
            {
                OuterValues outerValues = context.getOperatorContext().getOuterIndexValues().next();
                context.setTuple(outerValues.getOuterTuple());
                Object key = outerKeyExpression.eval(context);
                return new CacheKeyImpl(key, outerValues.getOuterTuple(), Arrays.copyOf(outerValues.getValues(), outerValues.getValues().length));
            }

            @Override
            public boolean hasNext()
            {
                return context.getOperatorContext().getOuterIndexValues().hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof OuterValuesCacheOperator)
        {
            OuterValuesCacheOperator that = (OuterValuesCacheOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator)
                && nameExpression.equals(that.nameExpression)
                && outerKeyExpression.equals(that.outerKeyExpression)
                && innerKeyExpression.equals(that.innerKeyExpression)
                && ttlExpression.equals(that.ttlExpression);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("OUTER VALUES CACHE (ID: %d, TTL: %s, OUTER KEY: %s, INNER KEY: %s)", nodeId, ttlExpression.toString(), outerKeyExpression.toString(), innerKeyExpression.toString())
            + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Cache key */
    static class CacheKeyImpl implements TupleCacheProvider.CacheKey
    {
        private final Object cacheKey;
        private final Object[] outerValues;
        private final Tuple outerTuple;

        CacheKeyImpl(Object cacheKey, Tuple outerTuple, Object[] outerValues)
        {
            this.cacheKey = cacheKey;
            this.outerTuple = outerTuple;
            this.outerValues = outerValues;
        }

        @Override
        public Object getKey()
        {
            return cacheKey;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(cacheKey);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof CacheKeyImpl)
            {
                return Objects.equals(cacheKey, ((CacheKeyImpl) obj).cacheKey);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return cacheKey.toString();
        }
    }
}
