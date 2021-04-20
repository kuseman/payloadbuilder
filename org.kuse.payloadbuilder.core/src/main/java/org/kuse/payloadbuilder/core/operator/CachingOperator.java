package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.operator.StatementContext.NodeData;

/** Caches provided operator to allow rewinds (Used in inner operator for nested loop) */
class CachingOperator extends AOperator
{
    private final Operator operator;

    /* Statistics */
    private int executionCount;

    CachingOperator(int nodeId, Operator target)
    {
        super(nodeId);
        this.operator = requireNonNull(target, "operator");
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public String getName()
    {
        return "Cache";
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        executionCount++;

        Data data = context.getStatementContext().getOrCreateNodeData(nodeId, Data::new);

        if (data.cache == null)
        {
            data.cache = new ArrayList<>();
            RowIterator it = operator.open(context);
            while (it.hasNext())
            {
                data.cache.add(it.next());
            }
            it.close();
        }
        return RowIterator.wrap(data.cache.iterator());
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
        return String.format("CACHING (ID: %d, EXECUTION COUNT: %d)", nodeId, executionCount) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Context data for this operator */
    static class Data extends NodeData
    {
        List<Tuple> cache;
    }
}
