package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

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
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }
    
    @Override
    public String getName()
    {
        return "Batch repeat";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry("Target nodeId", targetNodeId));
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
                        continue;
                    }
                    else if (!it.hasNext())
                    {
                        BatchLimitData batchLimitData = context.getOperatorContext().getNodeData(targetNodeId);
                        if (batchLimitData == null)
                        {
                            throw new OperatorException("Missing node data for target node id: " + targetNodeId);
                        }
                        
                        if (batchLimitData.isComplete())
                        {
                            return false;
                        }
                        
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
    public int hashCode()
    {
        return operator.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof BatchRepeatOperator)
        {
            BatchRepeatOperator that = (BatchRepeatOperator) obj;
            return operator.equals(that.operator)
                    && targetNodeId == that.targetNodeId;
        }
        return false;
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
    
    /** Definition of batch limit data. Provided by {@link BatchLimitOperator} and read
     * by {@link BatchRepeatOperator} */
    interface BatchLimitData
    {
        /** Returns true if the the limit operator complete */
        boolean isComplete();
    }
}