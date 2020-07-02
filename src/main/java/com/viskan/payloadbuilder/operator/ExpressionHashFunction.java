package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.stream.Collectors;

/** Function that generates a hash from provided expressions */
class ExpressionHashFunction implements ToIntBiFunction<ExecutionContext, Row>
{
    private final List<Expression> expressions;
    
    ExpressionHashFunction(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }
    
    @Override
    public int applyAsInt(ExecutionContext context, Row row)
    {
        int hash = 37;
        for (Expression expression : expressions)
        {
            context.setRow(row);
            Object result = expression.eval(context);
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
