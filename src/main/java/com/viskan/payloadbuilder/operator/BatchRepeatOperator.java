package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;

/** Operator that executes downstream operator in batches.
 * Continues to open down stream multiple times until end is reached after opening
 * Is used in conjunction with {@link BatchLimitOperator} 
 **/
class BatchRepeatOperator extends AOperator
{
    private final Operator operator;
    private final int targetNodeId;
    
    BatchRepeatOperator(int nodeId, int targetNodeId, Operator operator)
    {
        super(nodeId);
        this.targetNodeId = targetNodeId;
        this.operator = requireNonNull(operator, "operator");
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        return new Iterator<Row>()
        {
            private Iterator<Row> it;
            private Row next;
            
            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }
            
            @Override
            public boolean hasNext()
            {
                return setNext();
            }
            
            private boolean setNext()
            {
                while (next == null)
                {
                    if (it == null)
                    {
                        it = operator.open(context);
                        if (!it.hasNext())
                        {
                            return false;
                        }
                        continue;
                    }
                    else if (!it.hasNext())
                    {
                        it = null;
                        continue;
                    }
                    
                    next = it.next();
                }
                
                return true;
            }
        };
    }
    
    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("BATCH REPEATING (ID: %d, TARGET_ID: %d)", nodeId, targetNodeId);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }    
}
