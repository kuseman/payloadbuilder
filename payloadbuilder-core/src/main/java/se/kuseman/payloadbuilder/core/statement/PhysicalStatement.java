package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;

/** Physical statement that wraps a {@link IPhysicalPlan}. */
public class PhysicalStatement extends Statement
{
    private final IPhysicalPlan plan;
    private final Location location;

    public PhysicalStatement(IPhysicalPlan plan, Location location)
    {
        this.plan = requireNonNull(plan, "plan");
        this.location = location;
    }

    public IPhysicalPlan getPlan()
    {
        return plan;
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
        return plan.hashCode();
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
        else if (obj instanceof PhysicalStatement that)
        {
            return plan.equals(that.plan);
        }
        return false;
    }
}
