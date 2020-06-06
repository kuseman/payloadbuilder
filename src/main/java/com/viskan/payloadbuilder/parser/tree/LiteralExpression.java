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
    public boolean isConstant()
    {
        return true;
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
    
    /** Create a liteal expression from provided value */
    public static LiteralExpression create(Object value)
    {
        if (value == null)
        {
            return LiteralNullExpression.NULL_LITERAL;
        }
        else if (value instanceof String)
        {
            return new LiteralStringExpression((String) value);
        }
        else if (value instanceof Boolean)
        {
            return (Boolean) value ? LiteralBooleanExpression.TRUE_LITERAL : LiteralBooleanExpression.FALSE_LITERAL;
        }
        else if (value instanceof Double || value instanceof Float)
        {
            return new LiteralDecimalExpression(((Number) value).doubleValue());
        }
        else if (value instanceof Long || value instanceof Integer)
        {
            return new LiteralNumericExpression(((Number) value).longValue());
        }
        
        throw new IllegalArgumentException("Cannot create a literal expression from value " + value);
    }
}
