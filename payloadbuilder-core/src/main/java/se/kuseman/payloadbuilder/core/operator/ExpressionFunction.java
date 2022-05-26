package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;

import se.kuseman.payloadbuilder.core.codegen.BaseFunction;
import se.kuseman.payloadbuilder.core.parser.Expression;

/** Function that operates over an expression */
class ExpressionFunction implements Function<ExecutionContext, Object>
{
    private final Expression expression;

    ExpressionFunction(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public Object apply(ExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionFunction)
        {
            return expression.equals(((ExpressionFunction) obj).expression);
        }
        else if (obj instanceof BaseFunction)
        {
            // Equal against BaseFunction to get easier test code
            return expression.equals(((BaseFunction) obj).getExpression());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString();
    }
}
