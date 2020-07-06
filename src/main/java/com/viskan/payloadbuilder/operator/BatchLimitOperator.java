package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.DescribeUtils;
import com.viskan.payloadbuilder.operator.BatchRepeatOperator.BatchLimitData;
import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

/** Operator that limits number of rows in batches.
 * Is used in conjunction with {@link BatchRepeatOperator} 
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
    public Iterator<Row> open(ExecutionContext context)
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
        Iterator<Row> it = data.iterator;
        return new Iterator<Row>()
        {
            @Override
            public Row next()
            {
                data.count.increment();
                Row row = it.next();
                return row;
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
        Iterator<Row> iterator;
        MutableInt count = new MutableInt();
        int limit;
        
        @Override
        public boolean isComplete()
        {
            return !iterator.hasNext();
        }
    }
}
