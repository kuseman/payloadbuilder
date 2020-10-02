package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.operator.BatchRepeatOperator.BatchLimitData;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Operator that limits number of rows in batches. Is used in conjunction with {@link BatchRepeatOperator}
 **/
class BatchLimitOperator extends AOperator
{
    private final Operator operator;
    private final Expression batchLimitExpression;

    BatchLimitOperator(int nodeId, Operator operator, Expression batchLimitExpression)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.batchLimitExpression = requireNonNull(batchLimitExpression, "batchLimitExpression");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }

    @Override
    public String getName()
    {
        return "Batch limit";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(DescribeUtils.BATCH_SIZE, batchLimitExpression));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        Data data = context.getOperatorContext().getNodeData(nodeId, Data::new);
        if (data.iterator == null)
        {
            Object obj = batchLimitExpression.eval(context);
            if (!(obj instanceof Integer) || (Integer) obj < 0)
            {
                throw new OperatorException("Batch limit expression " + batchLimitExpression + " should return a positive Integer. Got: " + obj);
            }
            data.limit = (int) obj;
            data.iterator = operator.open(context);
        }
        else
        {
            data.count.setValue(0);
        }
        RowIterator it = data.iterator;
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                data.count.increment();
                Tuple tuple = it.next();
                return tuple;
            }

            @Override
            public void close()
            {
                it.close();
            }

            @Override
            public boolean hasNext()
            {
                if (!it.hasNext() || data.count.intValue() >= data.limit)
                {
                    return false;
                }
                return true;
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
        if (obj instanceof BatchLimitOperator)
        {
            BatchLimitOperator that = (BatchLimitOperator) obj;
            return operator.equals(that.operator)
                && batchLimitExpression.equals(that.batchLimitExpression);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("BATCH LIMIT (ID: %d, EXECUTION COUNT: %s, LIMIT: %s)", nodeId, null, batchLimitExpression);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Data for this operator */
    private static class Data extends NodeData implements BatchLimitData
    {
        RowIterator iterator;
        MutableInt count = new MutableInt();
        int limit;

        @Override
        public boolean isComplete()
        {
            return !iterator.hasNext();
        }
    }
}
