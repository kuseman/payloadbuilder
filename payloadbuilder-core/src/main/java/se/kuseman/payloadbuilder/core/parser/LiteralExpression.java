package se.kuseman.payloadbuilder.core.parser;

import java.util.Objects;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Base literal expression */
// CSOFF
public abstract class LiteralExpression extends Expression
// CSON
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
    public boolean isCodeGenSupported()
    {
        return true;
    }

    @Override
    public boolean isConstant()
    {
        return true;
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        return objectValue;
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        return 17 * 37 + (objectValue != null ? objectValue.hashCode()
                : 0);
        // CSON
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
            return (Boolean) value ? LiteralBooleanExpression.TRUE_LITERAL
                    : LiteralBooleanExpression.FALSE_LITERAL;
        }
        else if (value instanceof Double
                || value instanceof MutableDouble)
        {
            return new LiteralDoubleExpression(((Number) value).doubleValue());
        }
        else if (value instanceof Float
                || value instanceof MutableFloat)
        {
            return new LiteralFloatExpression(((Number) value).floatValue());
        }
        else if (value instanceof Long
                || value instanceof MutableLong)
        {
            return new LiteralLongExpression(((Number) value).longValue());
        }
        else if (value instanceof Integer
                || value instanceof MutableInt)
        {
            return new LiteralIntegerExpression(((Number) value).intValue());
        }

        throw new IllegalArgumentException("Cannot create a literal expression from value " + value);
    }

    /** Create a literal numeric expression from provided string */
    public static LiteralExpression createLiteralNumericExpression(String value)
    {
        char forceChar = value.charAt(value.length() - 1);
        if (forceChar == 'l'
                || forceChar == 'L')
        {
            return new LiteralLongExpression(value.substring(0, value.length() - 1));
        }
        try
        {
            return new LiteralIntegerExpression(value);
        }
        catch (NumberFormatException e)
        {
            try
            {
                return new LiteralLongExpression(value);
            }
            catch (NumberFormatException ee)
            {
                throw new RuntimeException("Cannot create a numeric expression out of " + value, ee);
            }
        }
    }

    /** Create a literal decimal expression from provided string */
    public static LiteralExpression createLiteralDecimalExpression(String value)
    {
        char forceChar = value.charAt(value.length() - 1);
        if (forceChar == 'f'
                || forceChar == 'F')
        {
            return new LiteralFloatExpression(value.substring(0, value.length() - 1));
        }
        else if (forceChar == 'd'
                || forceChar == 'D')
        {
            return new LiteralDoubleExpression(value.substring(0, value.length() - 1));
        }
        try
        {
            return new LiteralFloatExpression(value);
        }
        catch (NumberFormatException e)
        {
            try
            {
                return new LiteralDoubleExpression(value);
            }
            catch (NumberFormatException ee)
            {
                throw new RuntimeException("Cannot create a decimal expression out of " + value, ee);
            }
        }
    }
}
