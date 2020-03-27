package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.utils.IteratorUtils;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;

/** Operator that operates over an expression (that returns rows) */
public class ExpressionOperator implements Operator
{
    private final Expression expression;

    public ExpressionOperator(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        Object result = expression.eval(context.getEvaluationContext(), context.getParentProjectionRow());
        return result == null ? emptyIterator() : IteratorUtils.getIterator(result);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionOperator)
        {
            return expression.equals(((ExpressionOperator) obj).expression);
        }
        return false;
    }
}
