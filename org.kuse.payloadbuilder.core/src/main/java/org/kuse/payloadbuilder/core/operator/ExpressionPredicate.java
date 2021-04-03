package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.function.BiPredicate;

import org.apache.commons.lang3.BooleanUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Predicate that operates over an expression */
class ExpressionPredicate implements BiPredicate<ExecutionContext, Tuple>
{
    private final Expression predicate;

    ExpressionPredicate(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }

    @Override
    public boolean test(ExecutionContext context, Tuple tuple)
    {
        context.setTuple(tuple);
        boolean result = BooleanUtils.isTrue((Boolean) predicate.eval(context));
        context.setTuple(null);
        return result;
    }

    @Override
    public int hashCode()
    {
        return predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionPredicate)
        {
            return predicate.equals(((ExpressionPredicate) obj).predicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return predicate.toString();
    }
}
