package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;

/** Wrapper class for data type {@link Column.Type#Decimal}. */
public class Decimal extends Number implements Comparable<Decimal>, ValueVector
{
    /** NOTE! PLB doesn't have support for specifying scaling of decimals (unless created with String/BigDecinal), scale is set to this in factory methods */
    public static final int SCALE = 6;

    private final BigDecimal value;

    private Decimal(BigDecimal value)
    {
        this.value = requireNonNull(value, "value");
    }

    // ValueVector

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.of(Type.Decimal);
    }

    @Override
    public boolean isNull(int row)
    {
        return false;
    }

    @Override
    public Decimal getDecimal(int row)
    {
        return this;
    }

    // End ValueVector

    /** Process provided deicmal to this instance with provided arithmetic type. Returns a new {@link Decimal} */
    public Decimal processArithmetic(Decimal other, IArithmeticBinaryExpression.Type type)
    {
        switch (type)
        {
            case ADD:
                return new Decimal(value.add(other.value));
            case DIVIDE:
                return new Decimal(value.divide(other.value, RoundingMode.HALF_UP));
            case MODULUS:
                return new Decimal(value.remainder(other.value));
            case MULTIPLY:
                return new Decimal(value.multiply(other.value));
            case SUBTRACT:
                return new Decimal(value.subtract(other.value));
            default:
                throw new IllegalArgumentException("Unsupported arithmetic operation " + type);
        }
    }

    /** Negate this decimal returning a new instance */
    public Decimal negate()
    {
        return new Decimal(value.negate());
    }

    /** Convert this value as a {@link BigDecimal} */
    public BigDecimal asBigDecimal()
    {
        return value;
    }

    /** Return the absolute value of this decimal */
    public Decimal abs()
    {
        return Decimal.from(value.abs());
    }

    /** Return ceiling value of this decimal */
    public Decimal ceiling()
    {
        return Decimal.from(value.setScale(0, RoundingMode.CEILING));
    }

    /** Return floor value of this decimal */
    public Decimal floor()
    {
        return Decimal.from(value.setScale(0, RoundingMode.FLOOR));
    }

    @Override
    public int intValue()
    {
        return value.intValue();
    }

    @Override
    public long longValue()
    {
        return value.longValue();
    }

    @Override
    public float floatValue()
    {
        return value.floatValue();
    }

    @Override
    public double doubleValue()
    {
        return value.doubleValue();
    }

    @Override
    public int compareTo(Decimal o)
    {
        return value.compareTo(o.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof Decimal)
        {
            Decimal that = (Decimal) obj;
            return value.equals(that.value);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return value.toString();
    }

    /** Construct a deciaml from provided object. Tries to convert known types like BigDecimal/Strings etc. */
    public static Decimal from(Object value)
    {
        if (value instanceof Decimal)
        {
            return (Decimal) value;
        }
        else if (value instanceof BigDecimal)
        {
            return new Decimal((BigDecimal) value);
        }
        else if (value instanceof String
                || value instanceof UTF8String)
        {
            String str = value instanceof String ? (String) value
                    : ((UTF8String) value).toString();
            try
            {
                return new Decimal(new BigDecimal(str));
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Cannot cast '" + str + "' to " + Type.Decimal);
            }
        }
        else if (value instanceof Number)
        {
            // Use highest number precision value
            return new Decimal(new BigDecimal(((Number) value).doubleValue()).setScale(SCALE, RoundingMode.HALF_UP));
        }

        throw new IllegalArgumentException("Cannot cast '" + value + "' to " + Type.Decimal);
    }

    /** Construct a deciaml from provided int */
    public static Decimal from(int value)
    {
        return new Decimal(new BigDecimal(value).setScale(SCALE, RoundingMode.HALF_UP));
    }

    /** Construct a deciaml from provided long */
    public static Decimal from(long value)
    {
        return new Decimal(new BigDecimal(value).setScale(SCALE, RoundingMode.HALF_UP));
    }

    /** Construct a deciaml from provided float */
    public static Decimal from(float value)
    {
        return new Decimal(new BigDecimal(value).setScale(SCALE, RoundingMode.HALF_UP));
    }

    /** Construct a deciaml from provided double */
    public static Decimal from(double value)
    {
        return new Decimal(new BigDecimal(value).setScale(SCALE, RoundingMode.HALF_UP));
    }
}
