package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Function that generates a hash from provided expressions */
class ExpressionHashFunction implements ToIntBiFunction<ExecutionContext, Tuple>
{
    private static final int HASH_MULTIPLIER = 37;
    private static final int HASH_CONSTANT = 17;
    private final List<Expression> expressions;

    ExpressionHashFunction(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }

    @Override
    public int applyAsInt(ExecutionContext context, Tuple tuple)
    {
        context.setTuple(tuple);
        int hash = HASH_CONSTANT;
        for (Expression expression : expressions)
        {
            Object result = expression.eval(context);

            // If value is string and is digits, use the intvalue as
            // hash instead of string to be able to compare ints and strings
            // on left/right side of join
            if (result instanceof String && NumberUtils.isDigits((String) result))
            {
                result = Integer.parseInt((String) result);
            }
            hash = hash * HASH_MULTIPLIER + (result != null ? result.hashCode() : 0);
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
