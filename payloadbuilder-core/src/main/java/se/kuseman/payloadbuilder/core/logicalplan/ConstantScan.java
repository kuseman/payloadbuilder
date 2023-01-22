package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/** {@link se.kuseman.payloadbuilder.core.physicalplan.ConstantScan} */
public class ConstantScan implements ILogicalPlan
{
    public static final ConstantScan INSTANCE = new ConstantScan();

    private ConstantScan()
    {
    }

    @Override
    public Schema getSchema()
    {
        return Schema.EMPTY;
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof ConstantScan;
    }

    @Override
    public String toString()
    {
        return "Constant scan";
    }

}
