package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Plan for describing another physical plan and return describable output */
public class DescribePlan implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final boolean analyze;

    public DescribePlan(int nodeId, IPhysicalPlan input, boolean analyze)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.analyze = analyze;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return Schema.EMPTY;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // Execute and traverse query in analyze mode before gathering describe data
        if (analyze)
        {
            TupleIterator it = input.execute(context);
            try
            {
                while (it.hasNext())
                {
                    it.next();
                }
            }
            finally
            {
                it.close();
            }
        }

        return TupleIterator.singleton(DescribeUtils.getDescribeVector(context, input));
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
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
        else if (obj instanceof DescribePlan)
        {
            DescribePlan that = (DescribePlan) obj;
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && analyze == that.analyze;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (analyze ? "Analyze"
                : "Describe")
               + " Plan ("
               + nodeId
               + ")";
    }
}
