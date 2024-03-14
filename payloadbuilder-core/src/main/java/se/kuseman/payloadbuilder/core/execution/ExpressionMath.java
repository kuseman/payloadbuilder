package se.kuseman.payloadbuilder.core.execution;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;

/**
 * Math methods used when evaluating expressions
 */
public final class ExpressionMath
{
    private ExpressionMath()
    {
    }

    /** Compare. Requires both left and right to be non null */
    @SuppressWarnings({ "unchecked" })
    public static int cmp(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        if (left == right)
        {
            return 0;
        }

        // Handle known cases
        if (left instanceof Boolean a
                && right instanceof Boolean b)
        {
            return Boolean.compare(a, b);
        }
        else if (left instanceof UTF8String a
                && right instanceof UTF8String b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof EpochDateTime a
                && right instanceof EpochDateTime b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof EpochDateTimeOffset a
                && right instanceof EpochDateTimeOffset b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof Decimal a
                && right instanceof Decimal b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof Integer a
                && right instanceof Integer b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof Long a
                && right instanceof Long b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof Float a
                && right instanceof Float b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof Double a
                && right instanceof Double b)
        {
            return a.compareTo(b);
        }
        else if (left instanceof BigDecimal a
                && right instanceof BigDecimal b)
        {
            return a.compareTo(b);
        }

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            // Try comparable
            if (left instanceof Comparable)
            {
                return ((Comparable<Object>) left).compareTo(right);
            }
            else if (right instanceof Comparable)
            {
                // Switch sign here since we switched left/right
                return -((Comparable<Object>) right).compareTo(left);
            }

            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);
        return VectorUtils.compare(leftLiteral, rightLiteral, type, 0, 0);
    }

    /** Add. Requires both left and right to be non null */
    public static Object add(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + left.getClass()
                    .getSimpleName()
                                               + " and "
                                               + right.getClass()
                                                       .getSimpleName());
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);

        switch (type)
        {
            case Decimal:
                return Decimal.from(leftLiteral.getDecimal(0)
                        .processArithmetic(rightLiteral.getDecimal(0), IArithmeticBinaryExpression.Type.ADD));
            case Int:
                return Math.addExact(leftLiteral.getInt(0), rightLiteral.getInt(0));
            case Long:
                return Math.addExact(leftLiteral.getLong(0), rightLiteral.getLong(0));
            case Float:
                return leftLiteral.getFloat(0) + rightLiteral.getFloat(0);
            case Double:
                return leftLiteral.getDouble(0) + rightLiteral.getDouble(0);
            case String:
                return UTF8String.concat(UTF8String.EMPTY, asList(leftLiteral.getString(0), rightLiteral.getString(0)));
            default:
                throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftLiteral.type()
                        .getType()
                                                   + " and "
                                                   + rightLiteral.type()
                                                           .getType());
        }
    }

    /** Subtract. Requires both left and right to be non null */
    public static Object subtract(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + left.getClass()
                    .getSimpleName()
                                               + " and "
                                               + right.getClass()
                                                       .getSimpleName());
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);

        switch (type)
        {
            case Decimal:
                return Decimal.from(leftLiteral.getDecimal(0)
                        .processArithmetic(rightLiteral.getDecimal(0), IArithmeticBinaryExpression.Type.SUBTRACT));
            case Int:
                return Math.subtractExact(leftLiteral.getInt(0), rightLiteral.getInt(0));
            case Long:
                return Math.subtractExact(leftLiteral.getLong(0), rightLiteral.getLong(0));
            case Float:
                return leftLiteral.getFloat(0) - rightLiteral.getFloat(0);
            case Double:
                return leftLiteral.getDouble(0) - rightLiteral.getDouble(0);
            default:
                throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftLiteral.type()
                        .getType()
                                                   + " and "
                                                   + rightLiteral.type()
                                                           .getType());
        }
    }

    /** Multiply. Requires both left and right to be non null */
    public static Object multiply(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + left.getClass()
                    .getSimpleName()
                                               + " and "
                                               + right.getClass()
                                                       .getSimpleName());
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);

        switch (type)
        {
            case Decimal:
                return Decimal.from(leftLiteral.getDecimal(0)
                        .processArithmetic(rightLiteral.getDecimal(0), IArithmeticBinaryExpression.Type.MULTIPLY));
            case Int:
                return Math.multiplyExact(leftLiteral.getInt(0), rightLiteral.getInt(0));
            case Long:
                return Math.multiplyExact(leftLiteral.getLong(0), rightLiteral.getLong(0));
            case Float:
                return leftLiteral.getFloat(0) * rightLiteral.getFloat(0);
            case Double:
                return leftLiteral.getDouble(0) * rightLiteral.getDouble(0);
            default:
                throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftLiteral.type()
                        .getType()
                                                   + " and "
                                                   + rightLiteral.type()
                                                           .getType());
        }
    }

    /** Divide. Requires both left and right to be non null */
    public static Object divide(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + left.getClass()
                    .getSimpleName()
                                               + " and "
                                               + right.getClass()
                                                       .getSimpleName());
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);

        switch (type)
        {
            case Decimal:
                return Decimal.from(leftLiteral.getDecimal(0)
                        .processArithmetic(rightLiteral.getDecimal(0), IArithmeticBinaryExpression.Type.DIVIDE));
            case Int:
                return Math.floorDiv(leftLiteral.getInt(0), rightLiteral.getInt(0));
            case Long:
                return Math.floorDiv(leftLiteral.getLong(0), rightLiteral.getLong(0));
            case Float:
                return leftLiteral.getFloat(0) / rightLiteral.getFloat(0);
            case Double:
                return leftLiteral.getDouble(0) / rightLiteral.getDouble(0);
            default:
                throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftLiteral.type()
                        .getType()
                                                   + " and "
                                                   + rightLiteral.type()
                                                           .getType());
        }
    }

    /** Modulo. Requires both left and right to be non null */
    public static Object modulo(Object left, Object right)
    {
        requireNonNull(left);
        requireNonNull(right);

        ValueVector leftLiteral = getLiteral(left);
        ValueVector rightLiteral = getLiteral(right);

        if (leftLiteral == null
                || rightLiteral == null)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + left.getClass()
                    .getSimpleName()
                                               + " and "
                                               + right.getClass()
                                                       .getSimpleName());
        }

        Type type = getHighestPrecedenceType(leftLiteral, rightLiteral);

        switch (type)
        {
            case Decimal:
                return Decimal.from(leftLiteral.getDecimal(0)
                        .processArithmetic(rightLiteral.getDecimal(0), IArithmeticBinaryExpression.Type.MODULUS));
            case Int:
                return Math.floorMod(leftLiteral.getInt(0), rightLiteral.getInt(0));
            case Long:
                return Math.floorMod(leftLiteral.getLong(0), rightLiteral.getLong(0));
            case Float:
                return leftLiteral.getFloat(0) % rightLiteral.getFloat(0);
            case Double:
                return leftLiteral.getDouble(0) % rightLiteral.getDouble(0);
            default:
                throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftLiteral.type()
                        .getType()
                                                   + " and "
                                                   + rightLiteral.type()
                                                           .getType());
        }
    }

    /** Negate */
    public static Object negate(Object value)
    {
        requireNonNull(value);

        // Transform BigDecimal into PLB value
        if (value instanceof BigDecimal)
        {
            value = Decimal.from(value);
        }

        if (value instanceof Decimal)
        {
            return ((Decimal) value).negate();
        }
        else if (value instanceof Double)
        {
            return -((Double) value).doubleValue();
        }
        else if (value instanceof Float)
        {
            return -((Float) value).floatValue();
        }
        else if (value instanceof Long)
        {
            return -((Long) value).longValue();
        }
        else if (value instanceof Integer)
        {
            return -((Integer) value).intValue();
        }

        throw new IllegalArgumentException("Cannot negate '" + value + "'");
    }

    /** Abs */
    public static Object abs(Object value)
    {
        requireNonNull(value);

        // Transform BigDecimal into PLB value
        if (value instanceof BigDecimal)
        {
            value = Decimal.from(((BigDecimal) value).abs());
        }

        if (value instanceof Decimal)
        {
            return ((Decimal) value).abs();
        }
        else if (value instanceof Double)
        {
            return Math.abs(((Double) value).doubleValue());
        }
        else if (value instanceof Float)
        {
            return Math.abs(((Float) value).floatValue());
        }
        else if (value instanceof Long)
        {
            return Math.abs(((Long) value).longValue());
        }
        else if (value instanceof Integer)
        {
            return Math.abs(((Integer) value).intValue());
        }

        throw new IllegalArgumentException("Cannot return absolute value of: '" + value + "'");
    }

    /** Ceiling */
    public static Object ceiling(Object value)
    {
        requireNonNull(value);

        // Transform BigDecimal into PLB value
        if (value instanceof BigDecimal)
        {
            value = Decimal.from(value);
        }

        if (value instanceof Decimal)
        {
            return ((Decimal) value).ceiling();
        }
        else if (value instanceof Double)
        {
            return Math.ceil(((Double) value).doubleValue());
        }
        else if (value instanceof Float)
        {
            return (float) Math.ceil(((Float) value).floatValue());
        }
        else if (value instanceof Long)
        {
            return value;
        }
        else if (value instanceof Integer)
        {
            return value;
        }

        throw new IllegalArgumentException("Cannot return ceiling value of: '" + value + "'");
    }

    /** Floor */
    public static Object floor(Object value)
    {
        requireNonNull(value);

        // Transform BigDecimal into PLB value
        if (value instanceof BigDecimal)
        {
            value = Decimal.from(value);
        }

        if (value instanceof Decimal)
        {
            return ((Decimal) value).floor();
        }
        else if (value instanceof Double)
        {
            return Math.floor(((Double) value).doubleValue());
        }
        else if (value instanceof Float)
        {
            return (float) Math.floor(((Float) value).floatValue());
        }
        else if (value instanceof Long)
        {
            return value;
        }
        else if (value instanceof Integer)
        {
            return value;
        }

        throw new IllegalArgumentException("Cannot return floor value of: '" + value + "'");
    }

    /** Return the highest precedence type from provided objects. */
    private static Column.Type getHighestPrecedenceType(ValueVector left, ValueVector right)
    {
        Column.Type typeLeft = left.type()
                .getType();
        Column.Type typeRight = right.type()
                .getType();

        if (typeRight.getPrecedence() > typeLeft.getPrecedence())
        {
            return typeRight;
        }
        return typeLeft;
    }

    /** Try to convert provided object as a literal vector */
    private static ValueVector getLiteral(Object obj)
    {
        // Transform known types to PLB type
        if (obj instanceof BigDecimal)
        {
            obj = Decimal.from(obj);
        }
        else if (obj instanceof String)
        {
            obj = UTF8String.from(obj);
        }

        if (obj instanceof Boolean)
        {
            return ValueVector.literalBoolean((Boolean) obj, 1);
        }
        else if (obj instanceof Integer)
        {
            return ValueVector.literalInt((Integer) obj, 1);
        }
        else if (obj instanceof Long)
        {
            return ValueVector.literalLong((Long) obj, 1);
        }
        else if (obj instanceof Float)
        {
            return ValueVector.literalFloat((Float) obj, 1);
        }
        else if (obj instanceof Double)
        {
            return ValueVector.literalDouble((Double) obj, 1);
        }
        else if (obj instanceof Decimal)
        {
            return ValueVector.literalDecimal((Decimal) obj, 1);
        }
        else if (obj instanceof UTF8String)
        {
            return ValueVector.literalString((UTF8String) obj, 1);
        }

        return null;
    }
}
