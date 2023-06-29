package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;

/** Logical select statement. Statement in the planning phase. Will be transformed in to a {@link PhysicalSelectStatement} further down the lane */
public class LogicalSelectStatement extends SelectStatement
{
    private final ILogicalPlan select;
    private final boolean assignmentSelect;

    public LogicalSelectStatement(ILogicalPlan select, boolean assignmentSelect)
    {
        this.select = requireNonNull(select, "select");
        this.assignmentSelect = assignmentSelect;
    }

    public ILogicalPlan getSelect()
    {
        return select;
    }

    public boolean isAssignmentSelect()
    {
        return assignmentSelect;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        throw new IllegalArgumentException("A logical select cannot be executed");
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return select.hashCode();
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
        else if (obj instanceof LogicalSelectStatement)
        {
            LogicalSelectStatement that = (LogicalSelectStatement) obj;
            return select.equals(that.select)
                    && assignmentSelect == that.assignmentSelect;

        }
        return false;
    }
}
