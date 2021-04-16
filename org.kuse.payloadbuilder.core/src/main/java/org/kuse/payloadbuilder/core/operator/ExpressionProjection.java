package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Projection that operates over an {@link Expression} */
class ExpressionProjection extends AProjection
{
    private final Expression expression;

    ExpressionProjection(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public String getName()
    {
        if (expression instanceof QualifiedReferenceExpression)
        {
            return "Column (" + expression + ")";
        }
        else if (expression instanceof DescribableNode)
        {
            return ((DescribableNode) expression).getName();
        }
        return "Expression (" + expression + ")";
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        if (expression instanceof DescribableNode)
        {
            return ((DescribableNode) expression).getChildNodes();
        }
        return emptyList();
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        if (expression instanceof DescribableNode)
        {
            return ((DescribableNode) expression).getDescribeProperties(context);
        }
        return emptyMap();
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        Object value = expression.eval(context);
        writeValue(writer, context, value);
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

    @Override
    public String toString()
    {
        return expression.toString();
    }
}
