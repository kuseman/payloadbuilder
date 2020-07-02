package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.utils.IteratorUtils;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;

/** Operator that operates over an expression (that returns rows) */
class ExpressionOperator extends AOperator
{
    private final Expression expression;

    ExpressionOperator(int nodeId, Expression expression)
    {
        super(nodeId);
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        Object result = expression.eval(context);
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
            ExpressionOperator that = (ExpressionOperator) obj;
            return nodeId == that.nodeId
                && expression.equals(that.expression);
        }
        return false;
    }
}
