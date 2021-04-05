package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.function.Predicate;

import org.apache.commons.lang3.BooleanUtils;
import org.kuse.payloadbuilder.core.codegen.BasePredicate;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Predicate that operates over an expression */
class ExpressionPredicate implements Predicate<ExecutionContext>
{
    private final Expression predicate;

    ExpressionPredicate(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }

    @Override
    public boolean test(ExecutionContext context)
    {
        return BooleanUtils.isTrue((Boolean) predicate.eval(context));
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
        else if (obj instanceof BasePredicate)
        {
            // Equal against BasePredicate to get easier test code
            return predicate.equals(((BasePredicate) obj).getExpression());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return predicate.toString();
    }
}
