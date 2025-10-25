package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;

/** Physical statement that wraps a {@link IPhysicalPlan}. */
public class PhysicalStatement extends Statement
{
    private final IPhysicalPlan plan;

    public PhysicalStatement(IPhysicalPlan plan)
    {
        this.plan = requireNonNull(plan, "plan");
    }

    public IPhysicalPlan getPlan()
    {
        return plan;
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
