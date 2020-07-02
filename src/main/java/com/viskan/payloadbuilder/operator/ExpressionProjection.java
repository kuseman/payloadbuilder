package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Objects.requireNonNull;

/** Projection that operates over an {@link Expression} */
class ExpressionProjection implements Projection
{
    private final Expression expression;

    ExpressionProjection(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        writer.writeValue(expression.eval(context));
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionProjection)
        {
            return expression.equals(((ExpressionProjection) obj).expression);
        }
        return false;
    }
}
