package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

/** Projection that operates over an {@link Expression} */
public class ExpressionProjection implements Projection
{
    private final Expression expression;

    public ExpressionProjection(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public void writeValue(OutputWriter writer, OperatorContext context, Row row)
    {
        writer.writeValue(expression.eval(context.getEvaluationContext(), row));
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
