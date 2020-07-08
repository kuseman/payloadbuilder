package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/** Operator sorting target operator */
class SortByOperator extends AOperator
{
    private final Operator target;
    private final RowComparator comparator;

    SortByOperator(int nodeId, Operator target, RowComparator comparator)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.comparator = requireNonNull(comparator, "comparator");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(target);
    }

    @Override
    public String getName()
    {
        return "Order by";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry("Values", comparator.toString()));
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        List<Row> rows = new ArrayList<>();
        Iterator<Row> it = target.open(context);
        while (it.hasNext())
        {
            rows.add(it.next());
        }
        Collections.sort(rows, (rowA, rowB) -> comparator.compare(context, rowA, rowB));
        return rows.iterator();
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SortByOperator)
        {
            SortByOperator that = (SortByOperator) obj;
            return target.equals(that.target)
                && comparator.equals(that.comparator);
        }

        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("ORDER BY (ID: %d, VALUES: %s)", nodeId, comparator) + System.lineSeparator()
            +
            indentString + target.toString(indent + 1);
    }
}
