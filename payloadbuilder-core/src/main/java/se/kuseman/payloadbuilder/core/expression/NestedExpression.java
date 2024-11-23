package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.INestedExpression;

/** Nested expression */
public class NestedExpression implements INestedExpression
{
    private final IExpression expression;

    public NestedExpression(IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public IExpression fold()
    {
        // Only fold nested expressions to remove excessive parenthesis
        if (expression instanceof NestedExpression)
        {
            return expression.fold();
        }
        return expression;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public ResolvedType getType()
    {
        return expression.getType();
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return expression.eval(input, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
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
        else if (obj instanceof NestedExpression)
        {
            return expression.equals(((NestedExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "(" + expression.toString() + ")";
    }

    @Override
    public String toVerboseString()
    {
        return "(" + expression.toVerboseString() + ")";
    }
}
