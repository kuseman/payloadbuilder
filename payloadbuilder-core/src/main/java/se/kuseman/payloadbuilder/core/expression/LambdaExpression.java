package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILambdaExpression;

/** Lambda expression */
public class LambdaExpression implements ILambdaExpression
{
    private final List<String> identifiers;
    private final IExpression expression;
    /**
     * <pre>
     *  Unique ids for identifying lambda identifiers.
     * Used to retrieve the underlying value contained in the identifier
     * during evaluation.
     * </pre>
     */
    private final int[] lambdaIds;

    public LambdaExpression(List<String> identifiers, IExpression expression, int[] lambdaIds)
    {
        this.identifiers = requireNonNull(identifiers, "identifiers");
        this.expression = requireNonNull(expression);
        this.lambdaIds = lambdaIds;
    }

    @Override
    public List<String> getIdentifiers()
    {
        return identifiers;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    public int[] getLambdaIds()
    {
        return lambdaIds;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
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
    public ResolvedType getType()
    {
        throw new IllegalArgumentException("A lambda expression has no type");
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new IllegalArgumentException("A lambda expression cannot be evaluated");
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
        else if (obj instanceof LambdaExpression)
        {
            LambdaExpression le = (LambdaExpression) obj;
            return identifiers.equals(le.identifiers)
                    && expression.equals(le.expression)
                    && Arrays.equals(lambdaIds, le.lambdaIds);
        }
        return false;
    }

    @Override
    public String toVerboseString()
    {
        return (identifiers.size() > 1 ? "("
                : "")
               + identifiers.stream()
                       .map(i -> i.toString())
                       .collect(joining(","))
               + (identifiers.size() > 1 ? ")"
                       : "")
               + " -> "
               + expression.toVerboseString();
    }

    @Override
    public String toString()
    {
        return (identifiers.size() > 1 ? "("
                : "")
               + identifiers.stream()
                       .map(i -> i.toString())
                       .collect(joining(","))
               + (identifiers.size() > 1 ? ")"
                       : "")
               + " -> "
               + expression.toString();
    }
}
