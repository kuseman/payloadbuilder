package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Caches provided operator to allow rewinds (Used in inner operator for nested loop) */
public class CachingOperator extends AOperator
{
    private final Operator operator;

    /* Statistics */
    private int executionCount;

    public CachingOperator(int nodeId, Operator target)
    {
        super(nodeId);
        this.operator = requireNonNull(target, "operator");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        executionCount++;

        Data data = context.getNodeData(nodeId, Data::new);

        if (data.cache == null)
        {
            data.cache = new ArrayList<>();
            Iterator<Row> it = operator.open(context);
            while (it.hasNext())
            {
                data.cache.add(it.next());
            }
        }
        return data.cache.iterator();
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CachingOperator)
        {
            CachingOperator that = (CachingOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator);
        }
        return super.equals(obj);
    }

    @Override
    public String toString(int indent)
    {
        String indentString = repeat("  ", indent);
        return String.format("CACHING (ID: %d, EXECUTION COUNT: %d", nodeId, executionCount) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Context data for this operator */
    static class Data extends NodeData
    {
        List<Row> cache;
    }
}
