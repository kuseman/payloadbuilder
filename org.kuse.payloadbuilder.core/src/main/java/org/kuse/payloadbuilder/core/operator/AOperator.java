package org.kuse.payloadbuilder.core.operator;

/** Base class for operators */
public abstract class AOperator implements Operator
{
    protected final int nodeId;
    protected AOperator(int nodeId)
    {
        this.nodeId = nodeId;
    }
    
    @Override
    public int getNodeId()
    {
        return nodeId;
    }
}
