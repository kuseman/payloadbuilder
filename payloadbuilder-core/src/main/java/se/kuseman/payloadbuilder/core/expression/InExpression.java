package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.IInExpression;

/** expression IN (expression1, expression2 [, expressionN ]) */
public class InExpression implements IInExpression
{
    private final IExpression expression;
    private final List<IExpression> arguments;
    private final boolean not;

    public InExpression(IExpression expression, List<IExpression> arguments, boolean not)
    {
        this.expression = requireNonNull(expression, "expression");
        this.arguments = requireNonNull(arguments, "arguments");
        this.not = not;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public List<IExpression> getArguments()
    {
        return arguments;
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
        List<IExpression> children = new ArrayList<>();
        children.add(expression);
        children.addAll(arguments);
        return children;
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
        else if (obj instanceof InExpression)
        {
            InExpression that = (InExpression) obj;
            return expression.equals(that.expression)
                    && arguments.equals(that.arguments)
                    && not == that.not;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString() + (not ? " NOT"
                : "")
               + " IN ("
               + arguments.stream()
                       .map(IExpression::toString)
                       .collect(joining(", "))
               + ")";
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString() + (not ? " NOT"
                : "")
               + " IN ("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ")";
    }
}
