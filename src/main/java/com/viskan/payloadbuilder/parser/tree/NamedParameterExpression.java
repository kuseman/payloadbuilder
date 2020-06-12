package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

/** A named parameter (:value) */
public class NamedParameterExpression extends Expression
{
    private final String name;

    public NamedParameterExpression(String name)
    {
        this.name = requireNonNull(name, "name");
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }
    
    @Override
    public boolean isConstant()
    {
        return true;
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        return evaluationContext.getParameterValue(name);
    }
    
    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
 
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof NamedParameterExpression)
        {
            NamedParameterExpression that = (NamedParameterExpression) obj;
            return name.equals(that.name);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return ":" + name;
    }
}
