package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Operator for TOP expressions */
class TopOperator extends AOperator
{
    private final Operator target;
    private final Expression topExpression;

    TopOperator(int nodeId, Operator target, Expression topExpression)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.topExpression = requireNonNull(topExpression, "topExpression");
    }

    @Override
    public String getName()
    {
        return "TOP";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry("Value", topExpression));
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(target);
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        Object obj = topExpression.eval(context);
        if (!(obj instanceof Integer) || (Integer) obj < 0)
        {
            throw new OperatorException("Top expression " + topExpression + " should return a zero or positive Integer. Got: " + obj);
        }
        final int top = ((Integer) obj).intValue();
        final RowIterator it = target.open(context);
        final RowList list = it instanceof RowList ? (RowList) it : null;
        //CSOFF
        return new RowIterator()
        //CSON
        {
            private int count;

            @Override
            public Tuple next()
            {
                Tuple result = list == null ? it.next() : list.get(count);
                count++;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                if (list == null)
                {
                    return count < top && it.hasNext();
                }

                return count < top && count < list.size();
            }

            @Override
            public void close()
            {
                it.close();
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
                && topExpression.equals(that.topExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return super.toString();
    }
}
