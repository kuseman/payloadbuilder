package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Logical select statement. Statement in the planning phase. Will be transformed in to a {@link PhysicalStatement} further down the line */
public class LogicalSelectStatement extends Statement
{
    private final ILogicalPlan select;
    private final boolean assignmentSelect;
    private final Location location;

    public LogicalSelectStatement(ILogicalPlan select, boolean assignmentSelect, Location location)
    {
        this.select = requireNonNull(select, "select");
        this.assignmentSelect = assignmentSelect;
        this.location = location;
    }

    public ILogicalPlan getSelect()
    {
        return select;
    }

    public boolean isAssignmentSelect()
    {
        return assignmentSelect;
    }

    public Location getLocation()
    {
        return location;
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
        else if (obj instanceof LogicalSelectStatement that)
        {
            return select.equals(that.select)
                    && assignmentSelect == that.assignmentSelect;

        }
        return false;
    }
}
