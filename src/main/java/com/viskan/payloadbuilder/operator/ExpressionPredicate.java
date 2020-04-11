package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.function.BiPredicate;

import org.apache.commons.lang3.BooleanUtils;

/** Predicate that operates over an expression */
public class ExpressionPredicate implements BiPredicate<EvaluationContext, Row>
{
    private final Expression predicate;
    
    public ExpressionPredicate(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }
    
    @Override
    public boolean test(EvaluationContext context, Row row)
    {
        return BooleanUtils.isTrue((Boolean) predicate.eval(context, row));
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
