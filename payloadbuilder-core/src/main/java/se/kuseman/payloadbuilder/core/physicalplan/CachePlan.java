package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;

/**
 * Plan that caches the input and returns the cached values for each execution. Used for example in nested loops where we don't have index or correlation
 */
public class CachePlan implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;

    public CachePlan(int nodeId, IPhysicalPlan input)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public String getName()
    {
        return "Cache";
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final CacheNodeData nodeData = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new CacheNodeData());

        return new TupleIterator()
        {
            TupleVectorBuilder builder;
            TupleIterator it;
            boolean complete;

            @Override
            public TupleVector next()
            {
                TupleVector next;

                if (it != null)
                {
                    next = it.next();

                    if (builder == null)
                    {
                        int rowCount = Math.max(next.getRowCount(), it.estimatedRowCount());
                        builder = new TupleVectorBuilder(((ExecutionContext) context).getBufferAllocator(), rowCount);
                    }

                    builder.append(next);
                }
                else
                {
                    next = nodeData.tupleVector;
                    complete = true;
                }

                return next;
            }

            @Override
            public boolean hasNext()
            {
                // First execution, cache vectors
                if (nodeData.tupleVector == null)
                {
                    if (it == null)
                    {
                        it = input.execute(context);
                    }
                    return it.hasNext();
                }

                return !complete;
            }

            @Override
            public void close()
            {
                if (it != null)
                {
                    it.close();
                    nodeData.tupleVector = builder != null ? builder.build()
                            : TupleVector.EMPTY;
                }
            };
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return List.of(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CachePlan that)
        {
            return input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Cache (" + nodeId + ")";
    }

    static class CacheNodeData extends NodeData
    {
        TupleVector tupleVector;
    }
}
