package com.viskan.payloadbuilder.operator;

/** Base class for operators */
public abstract class AOperator implements Operator
{
    protected final int nodeId;
    public AOperator(int nodeId)
    {
        this.nodeId = nodeId;
    }
}
