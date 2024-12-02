package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * An unresolved sub query expression
 **/
public class UnresolvedSubQueryExpression implements IExpression
{
    private final ILogicalPlan input;
    private final Location location;

    public UnresolvedSubQueryExpression(ILogicalPlan input, Location location)
    {
        this.input = requireNonNull(input, "input");
        this.location = location;
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public ResolvedType getType()
    {
        // A sub query expression can only have one column so return it if present
        Schema schema = input.getSchema();
        if (schema.getSize() > 0)
        {
            return schema.getColumns()
                    .get(0)
                    .getType();
        }
        // else return any here, this will be resolved later on to the correct type when this sub query is eliminated from plan
        return ResolvedType.of(Type.Any);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        if (visitor instanceof ICoreExpressionVisitor)
        {
            return ((ICoreExpressionVisitor<T, C>) visitor).visit(this, context);
        }
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new IllegalArgumentException("A sub query expression cannot be evaluated");
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
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
        else if (obj instanceof UnresolvedSubQueryExpression that)
        {
            return input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return input.toString();
    }
}
