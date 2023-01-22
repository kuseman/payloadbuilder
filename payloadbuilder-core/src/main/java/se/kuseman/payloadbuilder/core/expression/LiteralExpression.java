package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Base class for literal expressions */
public abstract class LiteralExpression implements IExpression
{
    protected final ResolvedType type;

    public LiteralExpression(ResolvedType type)
    {
        this.type = requireNonNull(type);
    }

    @Override
    public ResolvedType getType()
    {
        return type;
    }

    @Override
    public boolean isConstant()
    {
        return true;
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
