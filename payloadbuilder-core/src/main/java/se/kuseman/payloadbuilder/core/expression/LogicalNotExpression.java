package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILogicalNotExpression;
import se.kuseman.payloadbuilder.core.physicalplan.BitSetVector;

/** Logical NOT */
public class LogicalNotExpression implements ILogicalNotExpression
{
    private final IExpression expression;

    public LogicalNotExpression(IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ValueVector vector = expression.eval(input, context);
        return BitSetVector.not(vector);
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Boolean);
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
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
        else if (obj instanceof LogicalNotExpression)
        {
            return expression.equals(((LogicalNotExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "NOT " + expression;
    }
}
