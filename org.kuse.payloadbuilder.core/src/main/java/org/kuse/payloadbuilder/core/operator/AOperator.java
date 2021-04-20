package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

/** Base class for operators */
public abstract class AOperator implements Operator
{
    protected final Integer nodeId;

    protected AOperator(Integer nodeId)
    {
        this.nodeId = requireNonNull(nodeId);
    }

    @Override
    public int getNodeId()
    {
        return nodeId.intValue();
    }
}
