package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/**
 * Plan that assigns the result to variables and have no output.
 * 
 * <pre>
 * select @var = col
 * from table
 * </pre>
 */
public class AssignmentPlan implements IPhysicalPlan
{
    private final IPhysicalPlan input;
    private final int nodeId;

    public AssignmentPlan(int nodeId, IPhysicalPlan input)
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
    public String getName()
    {
        return input.getName();
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public boolean hasWritableOutput()
    {
        return false;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // Consume the input, to propagate the assignment expressions
        TupleIterator iterator = input.execute(context);
        int rowCount = 0;
        try
        {
            while (iterator.hasNext())
            {
                rowCount += iterator.next()
                        .getRowCount();
            }
        }
        finally
        {
            iterator.close();
        }

        ((StatementContext) context.getStatementContext()).setRowCount(rowCount);
        return TupleIterator.EMPTY;
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<? extends DescribableNode> getChildNodes()
    {
        /// Skip this node since it's only a wrapper around a plain select
        return input.getChildNodes();
    }
}
