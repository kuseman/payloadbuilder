package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.stream.Collectors;

/** Function that generates a hash from provided expressions */
public class ExpressionHashFunction implements ToIntBiFunction<EvaluationContext, Row>
{
    private final List<Expression> expressions;
    
    public ExpressionHashFunction(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }
    
    @Override
    public int applyAsInt(EvaluationContext context, Row row)
    {
        int hash = 37;
        for (Expression expression : expressions)
        {
            Object result = expression.eval(context, row);
            hash += 17 * (result != null ? result.hashCode() : 0);
        }
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
