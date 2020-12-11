package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.CacheProvider;
import org.kuse.payloadbuilder.core.operator.OperatorContext.OuterValues;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/**
 * Cache operator that sits between a Batch operator and the inner operator to allow for external cache of values
 */
class OuterValuesCacheOperator extends AOperator
{
    private final Operator operator;
    /** TTL expression in https://en.wikipedia.org/wiki/ISO_8601#Durations format */
    private final Expression ttlExpression;
    /** Cache key for the outer relation */
    private final Expression outerKeyExpression;
    /** Cache key for the inner relation */
    private final Expression innerKeyExpression;

    OuterValuesCacheOperator(
            int nodeId,
            Operator operator,
            Expression ttlExpression,
            Expression outerKeyExpression,
            Expression innerKeyExpression)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.ttlExpression = requireNonNull(ttlExpression, "ttlExpression");
        this.outerKeyExpression = requireNonNull(outerKeyExpression, "outerKeyExpression");
        this.innerKeyExpression = requireNonNull(innerKeyExpression, "innerKeyExpression");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return singletonList(operator);
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return MapUtils.ofEntries(
                MapUtils.entry("Outer key", outerKeyExpression.toString()),
                MapUtils.entry("Inner key", innerKeyExpression.toString()),
                MapUtils.entry("TTL", ttlExpression.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        CacheProvider cacheProvider = context.getSession().getCacheProvider();
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

        Map<CacheKeyImpl, List<Tuple>> cachedValues = cacheProvider.getAll(getCacheKeyIterable(context));
        Map<Object, List<Tuple>> nonCachedValues = getAndCache(context, cacheProvider, ttl, cachedValues);

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
            CacheProvider cacheProvider,
            Duration ttl,
            Map<CacheKeyImpl, List<Tuple>> cachedValues)
    {
        Iterator<OuterValues> outerValuesIterator = getOuterValuesIterator(cachedValues);
        // No values to fetch, everything was present in cache
        if (!outerValuesIterator.hasNext())
        {
            return emptyMap();
        }
        context.getOperatorContext().setOuterIndexValues(outerValuesIterator);
        RowIterator it = operator.open(context);

        Map<Object, List<Tuple>> values = new LinkedHashMap<>();
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            context.setTuple(tuple);
            Object key = innerKeyExpression.eval(context);
            values.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }
        it.close();

        cacheProvider.putAll(values, ttl);

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
     */
    private Iterable<CacheKeyImpl> getCacheKeyIterable(ExecutionContext context)
    {
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
                && ttlExpression.equals(that.ttlExpression)
                && outerKeyExpression.equals(that.outerKeyExpression)
                && innerKeyExpression.equals(that.innerKeyExpression);
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
    static class CacheKeyImpl implements CacheProvider.CacheKey
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
