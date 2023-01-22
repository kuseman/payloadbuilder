package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** A scan operator returning one row with no schema. Used for example when having a query without a from clause etc. */
public class ConstantScan implements IPhysicalPlan
{
    private final int nodeId;

    public ConstantScan(int nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Constant Scan";
    }

    @Override
    public Schema getSchema()
    {
        return Schema.EMPTY;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        return TupleIterator.singleton(TupleVector.CONSTANT);
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof ConstantScan)
        {
            ConstantScan that = (ConstantScan) obj;
            return nodeId == that.nodeId;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Constant scan (" + nodeId + ")";
    }
}
