package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;

/** Physical select statement */
public class PhysicalSelectStatement extends SelectStatement
{
    private final IPhysicalPlan select;

    public PhysicalSelectStatement(IPhysicalPlan select)
    {
        this.select = requireNonNull(select, "select");
    }

    public IPhysicalPlan getSelect()
    {
        return select;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        return select.execute(context);
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
