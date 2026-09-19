package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.execution.LogicalBinaryCoverageData;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Wraps a {@link LogicalBinaryExpression} to track how many rows the condition evaluates to {@code true} vs. {@code false} during test execution. Delegates all evaluation to the wrapped expression;
 * the instrumentation is a counting pass over the result vector.
 */
public class InstrumentedLogicalBinaryExpression implements IExpression
{
    private final int nodeId;
    private final LogicalBinaryExpression wrapped;
    private final Location location;

    public InstrumentedLogicalBinaryExpression(int nodeId, LogicalBinaryExpression wrapped)
    {
        this.nodeId = nodeId;
        this.wrapped = requireNonNull(wrapped, "wrapped");
        this.location = wrapped.getLocation();
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public LogicalBinaryExpression getWrapped()
    {
        return wrapped;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public ResolvedType getType()
    {
        return wrapped.getType();
    }

    @Override
    public List<IExpression> getChildren()
    {
        return wrapped.getChildren();
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return wrapped.accept(visitor, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        ValueVector result = wrapped.eval(input, selection, context);

        LogicalBinaryCoverageData data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new LogicalBinaryCoverageData(wrapped.getLogicalType() + " " + wrapped.getRight(), location));

        int size = result.size();
        long trueCount = 0;
        long falseCount = 0;
        for (int i = 0; i < size; i++)
        {
            if (!result.isNull(i))
            {
                if (result.getBoolean(i))
                {
                    trueCount++;
                }
                else
                {
                    falseCount++;
                }
            }
        }
        data.recordHits(trueCount, falseCount);

        return result;
    }

    @Override
    public String toString()
    {
        return wrapped.toString();
    }
}
