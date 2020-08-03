package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Operator for TOP expressions */
class TopOperator extends AOperator
{
    private final Operator target;
    private final int top;

    TopOperator(int nodeId, Operator target, int top)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.top = top;
    }

    @Override
    public String getName()
    {
        return "TOP";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return MapUtils.ofEntries(MapUtils.entry("Value", top));
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return singletonList(target);
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        final Iterator<Row> it = target.open(context);
        return new Iterator<Row>()
        {
            private int count;

            @Override
            public Row next()
            {
                count++;
                return it.next();
            }

            @Override
            public boolean hasNext()
            {
                return count < top && it.hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TopOperator)
        {
            TopOperator that = (TopOperator) obj;
            return target.equals(that.target)
                && top == that.top;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return super.toString();
    }
}
