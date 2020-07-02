package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Objects.requireNonNull;

import java.util.function.BiPredicate;

import org.apache.commons.lang3.BooleanUtils;

/** Predicate that operates over an expression */
class ExpressionPredicate implements BiPredicate<ExecutionContext, Row>
{
    private final Expression predicate;
    
    ExpressionPredicate(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }
    
    @Override
    public boolean test(ExecutionContext context, Row row)
    {
        context.setRow(row);
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
        return false;
    }
    
    @Override
    public String toString()
    {
        return predicate.toString();
    }
}
