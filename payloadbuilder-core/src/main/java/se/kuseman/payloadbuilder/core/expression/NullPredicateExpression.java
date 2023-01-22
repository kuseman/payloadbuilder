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
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/** IS (NOT?) NULL expression */
public class NullPredicateExpression implements INullPredicateExpression
{
    private final IExpression expression;
    private final boolean not;

    public NullPredicateExpression(IExpression expression, boolean not)
    {
        this.expression = requireNonNull(expression, "expression");
        this.not = not;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isNot()
    {
        return not;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new RuntimeException("not implemented");
        // ValueVector vector = expression.eval(input, context);
        // return BitSetVector.not(vector);
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
        else if (obj instanceof NullPredicateExpression)
        {
            NullPredicateExpression that = (NullPredicateExpression) obj;
            return expression.equals(that.expression)
                    && not == that.not;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString() + " IS"
               + (not ? " NOT"
                       : "")
               + " NULL";
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString() + " IS"
               + (not ? " NOT"
                       : "")
               + " NULL";
    }
}
