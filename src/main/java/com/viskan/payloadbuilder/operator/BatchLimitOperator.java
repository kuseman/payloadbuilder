package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;

/** Operator that limits number of rows in batches.
 * Is used in conjunction with {@link BatchRepeatOperator} 
 **/
class BatchLimitOperator extends AOperator
{
    private final Operator operator;
    private final int limit;
    
    BatchLimitOperator(int nodeId, Operator operator, int limit)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.limit = limit;
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        Data data = context.getOperatorContext().getNodeData(nodeId, Data::new);
        if (data.iterator == null)
        {
            data.iterator = operator.open(context);
        }
        Iterator<Row> it = data.iterator;
        return new Iterator<Row>()
        {
            private int count = 0;
            @Override
            public Row next()
            {
                count++;
                Row row = it.next();
                return row;
            }
            
            @Override
            public boolean hasNext()
            {
                if (!it.hasNext() || count >= limit)
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
        String desc = String.format("BATCH LIMIT (ID: %d, EXECUTION COUNT: %s, LIMIT: %s)", nodeId, null, limit);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }    
    /** Data for this operator */
    private static class Data extends NodeData
    {
        Iterator<Row> iterator;
    }
}
