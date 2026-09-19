package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.execution.CaseCoverageData;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.parser.Location;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Wraps a {@link CaseExpression} to track which WHEN branches are hit during test execution. A secondary pass evaluates WHEN conditions before the main call to count per-branch hits. */
public class InstrumentedCaseExpression implements IExpression
{
    private final int nodeId;
    private final CaseExpression wrapped;
    private final Location location;

    public InstrumentedCaseExpression(int nodeId, CaseExpression wrapped)
    {
        this.nodeId = nodeId;
        this.wrapped = requireNonNull(wrapped, "wrapped");
        this.location = wrapped.getLocation();
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public CaseExpression getWrapped()
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
        // Track which branches were hit using the sequential-matching algorithm
        List<ICaseExpression.WhenClause> whenClauses = wrapped.getWhenClauses();
        int size = whenClauses.size();

        CaseCoverageData data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new CaseCoverageData(size, wrapped.toString(), location));

        int rowCount = selection.size();
        IntList nonMatchedRows = new IntArrayList(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            nonMatchedRows.add(selection.getInt(i));
        }

        for (int j = 0; j < size; j++)
        {
            ValueVector nonMatchedSelection = VectorUtils.convertToSelectionVector(nonMatchedRows);
            ValueVector condition = whenClauses.get(j)
                    .getCondition()
                    .eval(input, nonMatchedSelection, context);

            int currentSize = nonMatchedRows.size();
            IntList matchedRows = new IntArrayList();
            for (int i = 0; i < currentSize; i++)
            {
                if (condition.getPredicateBoolean(i))
                {
                    matchedRows.add(nonMatchedRows.getInt(i));
                }
            }

            if (!matchedRows.isEmpty())
            {
                data.recordWhenHit(j, matchedRows.size());
                nonMatchedRows.removeAll(matchedRows);
            }
        }

        if (!nonMatchedRows.isEmpty())
        {
            data.recordElseHit(nonMatchedRows.size());
        }

        // Delegate to wrapped for the real result
        return wrapped.eval(input, selection, context);
    }

    @Override
    public String toString()
    {
        return wrapped.toString();
    }
}
