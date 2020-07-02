package com.viskan.payloadbuilder.operator;

/** Base class for operators */
abstract class AOperator implements Operator
{
    protected final int nodeId;
    AOperator(int nodeId)
    {
        this.nodeId = nodeId;
    }
}
