package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.parser.Expression;

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
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry("Value", topExpression));
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(target);
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        Object obj = topExpression.eval(context);
        if (!(obj instanceof Integer)
                || (Integer) obj < 0)
        {
            throw new OperatorException("Top expression " + topExpression + " should return a zero or positive Integer. Got: " + obj);
        }
        final int top = ((Integer) obj).intValue();

        return new ATupleIterator(target.open(context))
        {
            private int count;

            @Override
            public boolean hasNext()
            {
                return count >= top ? false
                        : super.hasNext();
            }

            @Override
            protected boolean setNext(Tuple tuple)
            {
                count++;
                return super.setNext(tuple);
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
