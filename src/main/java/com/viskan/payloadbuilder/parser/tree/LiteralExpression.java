package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import java.util.Objects;

public abstract class LiteralExpression extends Expression
{
    private final Object objectValue;

    protected LiteralExpression(Object value)
    {
        this.objectValue = value;
    }
    
    public Object getObjectValue()
    {
        return objectValue;
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        return objectValue;
    }

    @Override
    public boolean isNullable()
    {
        return objectValue == null;
    }
    
    @Override
    public Class<?> getDataType()
    {
        return objectValue != null ? objectValue.getClass() : super.getDataType();
    }
    
    @Override
    public int hashCode()
    {
        return 17 + 
                37 * (objectValue != null ? objectValue.hashCode() : 0);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LiteralExpression)
        {
            return Objects.equals(objectValue, ((LiteralExpression) obj).objectValue);
        }
        return false;
    }
}
