package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** {@link se.kuseman.payloadbuilder.core.physicalplan.Limit} */
public class Limit implements ILogicalPlan
{
    private final ILogicalPlan input;
    private final IExpression limitExpression;

    public Limit(ILogicalPlan input, IExpression limitExpression)
    {
        this.input = requireNonNull(input, "input");
        this.limitExpression = requireNonNull(limitExpression, "limitExpression");
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public IExpression getLimitExpression()
    {
        return limitExpression;
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Limit)
        {
            Limit that = (Limit) obj;
            return input.equals(that.input)
                    && limitExpression.equals(that.limitExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "LIMIT " + limitExpression.toVerboseString();
    }
}
