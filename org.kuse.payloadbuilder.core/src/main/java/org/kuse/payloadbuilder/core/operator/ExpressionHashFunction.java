package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.stream.Collectors;

import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Function that generates a hash from provided expressions */
class ExpressionHashFunction implements ToIntBiFunction<ExecutionContext, Tuple>
{
    private final List<Expression> expressions;

    ExpressionHashFunction(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }

    @Override
    public int applyAsInt(ExecutionContext context, Tuple tuple)
    {
        context.setTuple(tuple);
        int hash = ObjectUtils.HASH_CONSTANT;
        for (Expression expression : expressions)
        {
            Object result = expression.eval(context);
            hash = hash * ObjectUtils.HASH_MULTIPLIER + ObjectUtils.hash(result);
        }
        context.setTuple(null);
        return hash;
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionHashFunction)
        {
            return expressions.equals(((ExpressionHashFunction) obj).expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expressions.stream().map(Object::toString).collect(Collectors.joining(", "));
    }
}
