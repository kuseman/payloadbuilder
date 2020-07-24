package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator that groups by a bucket function */
class GroupByOperator extends AOperator
{
    private final Operator operator;
    private final ValuesExtractor valuesExtractor;
    private final int size;
    private final List<String> columnReferences;

    GroupByOperator(
            int nodeId,
            Operator operator,
            List<String> columnReferences,
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
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }
    
    @Override
    public String getName()
    {
        return "Group by";
    }
    
    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry("Values", valuesExtractor.toString())
                );
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        ArrayKey key = new ArrayKey();
        key.key = new Object[size];

        Map<ArrayKey, List<Row>> table = new LinkedHashMap<>();
        Iterator<Row> it = operator.open(context);
        while (it.hasNext())
        {
            Row row = it.next();
            valuesExtractor.extract(context, row, key.key);
            table.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        return new TransformIterator(table.values().iterator(), new Transformer()
        {
            private int position = 0;

            @Override
            public Object transform(Object input)
            {
                return new GroupedRow((List<Row>) input, position++, columnReferences);
            }
        });
    }

    @Override
    public int hashCode()
    {
        return 17
            + (37 * operator.hashCode())
            + (37 * valuesExtractor.hashCode())
            + (37 * size);
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
