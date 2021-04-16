package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/** Operator that groups by a bucket function */
class GroupByOperator extends AOperator
{
    private final Operator operator;
    private final ValuesExtractor valuesExtractor;
    private final int size;
    private final Map<Integer, Set<String>> columnReferences;

    GroupByOperator(
            int nodeId,
            Operator operator,
            Map<Integer, Set<String>> columnReferences,
            ValuesExtractor valuesExtractor,
            int size)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.columnReferences = requireNonNull(columnReferences, "columnReferences");
        this.valuesExtractor = requireNonNull(valuesExtractor, "valuesExtractor");
        this.size = size;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public String getName()
    {
        return "Group by";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(true,
                entry("Values", valuesExtractor.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        ArrayKey key = new ArrayKey();
        key.key = new Object[size];

        Map<ArrayKey, List<Tuple>> table = new LinkedHashMap<>();
        RowIterator it = operator.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            valuesExtractor.extract(context, tuple, key.key);
            table.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }
        it.close();

        Iterator<List<Tuple>> iterator = table.values().iterator();
        //CSOFF
        return new RowIterator()
        //CSON
        {
            Map<Integer, Set<Integer>> groupByOrdinals;

            @Override
            public Tuple next()
            {
                List<Tuple> tuples = iterator.next();
                // Calculate ordinals from the first tuples data
                if (groupByOrdinals == null)
                {
                    groupByOrdinals = new HashMap<>();
                    Tuple firstTuple = tuples.get(0);
                    for (Entry<Integer, Set<String>> e : columnReferences.entrySet())
                    {
                        // Count how many columns there are before current ordinal
                        Tuple tuple = firstTuple.getTuple(e.getKey());
                        groupByOrdinals.put(e.getKey(), e.getValue().stream().map(c -> tuple.getColumnOrdinal(c)).collect(toSet()));
                    }
                }
                return new GroupedRow(tuples, groupByOrdinals);
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
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
        if (obj instanceof GroupByOperator)
        {
            GroupByOperator that = (GroupByOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator)
                && columnReferences.equals(that.columnReferences)
                && valuesExtractor.equals(that.valuesExtractor)
                && size == that.size;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("GROUP BY (ID: %d, VALUES: %s)", nodeId, valuesExtractor) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /**
     * <pre>
     * NOTE! Special key class used in hash map.
     * To avoid allocations and use the same object array for each row a single instance is used of this class.
     * It's safe because the values are iterated after grouping and hence we don't need to find the values again by key.
     * </pre>
     **/
    private static class ArrayKey
    {
        private Object[] key;

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(key);
        }

        @Override
        public boolean equals(Object obj)
        {
            return true;
        }
    }
}
