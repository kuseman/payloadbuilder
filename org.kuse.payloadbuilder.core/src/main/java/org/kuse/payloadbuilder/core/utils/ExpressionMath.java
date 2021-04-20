package org.kuse.payloadbuilder.core.utils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;

/**
 * Math methods used when evaluating expressions TODO: Overflows aren't taken care of in arithmetic operations
 */
public final class ExpressionMath
{
    private ExpressionMath()
    {
    }

    private static final byte ONE = 1;
    private static final byte ZERO = 1;

    /** Double op */
    private interface DoubleOp
    {
        double apply(double left, double right);
    }

    /** Float op */
    private interface FloatOp
    {
        float apply(float left, float right);
    }

    /** Long op */
    private interface LongOp
    {
        long apply(long left, long right);
    }

    /** Int op */
    private interface IntOp
    {
        int apply(int left, int right);
    }

    private static final DoubleOp ADD_DOUBLE = (left, right) -> left + right;
    private static final FloatOp ADD_FLOAT = (left, right) -> left + right;
    private static final LongOp ADD_LONG = (left, right) -> left + right;
    private static final IntOp ADD_INT = (left, right) -> left + right;

    private static final DoubleOp SUB_DOUBLE = (left, right) -> left - right;
    private static final FloatOp SUB_FLOAT = (left, right) -> left - right;
    private static final LongOp SUB_LONG = (left, right) -> left - right;
    private static final IntOp SUB_INT = (left, right) -> left - right;

    private static final DoubleOp MUL_DOUBLE = (left, right) -> left * right;
    private static final FloatOp MUL_FLOAT = (left, right) -> left * right;
    private static final LongOp MUL_LONG = (left, right) -> left * right;
    private static final IntOp MUL_INT = (left, right) -> left * right;

    private static final DoubleOp DIV_DOUBLE = (left, right) -> left / right;
    private static final FloatOp DIV_FLOAT = (left, right) -> left / right;
    private static final LongOp DIV_LONG = (left, right) -> left / right;
    private static final IntOp DIV_INT = (left, right) -> left / right;

    private static final DoubleOp MOD_DOUBLE = (left, right) -> left % right;
    private static final FloatOp MOD_FLOAT = (left, right) -> left % right;
    private static final LongOp MOD_LONG = (left, right) -> left % right;
    private static final IntOp MOD_INT = (left, right) -> left % right;

    /** Greater than */
    @SuppressWarnings("unchecked")
    public static boolean gt(Object left, Object right)
    {
        if (left instanceof Number)
        {
            Number l = (Number) left;
            Number r = convertToNumber(l, right, true);
            if (r != null)
            {
                return cmp(l, r) > 0;
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return cmp(l, r) > 0;
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            byte a = ((Boolean) left).booleanValue() ? ONE : ZERO;
            byte b = ((Boolean) right).booleanValue() ? ONE : ZERO;
            return a - b > 0;
        }

        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right) > 0;
    }

    /** Greater than equal */
    @SuppressWarnings("unchecked")
    public static boolean gte(Object left, Object right)
    {
        if (left instanceof Number)
        {
            Number l = (Number) left;
            Number r = convertToNumber(l, right, true);
            if (r != null)
            {
                return cmp(l, r) >= 0;
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return cmp(l, r) >= 0;
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            byte a = ((Boolean) left).booleanValue() ? ONE : ZERO;
            byte b = ((Boolean) right).booleanValue() ? ONE : ZERO;
            return a - b >= 0;
        }

        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right) >= 0;
    }

    /** Less than equal */
    @SuppressWarnings("unchecked")
    public static boolean lt(Object left, Object right)
    {
        if (left instanceof Number)
        {
            Number l = (Number) left;
            Number r = convertToNumber(l, right, true);
            if (r != null)
            {
                return cmp(l, r) < 0;
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return cmp(l, r) < 0;
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            byte a = ((Boolean) left).booleanValue() ? ONE : ZERO;
            byte b = ((Boolean) right).booleanValue() ? ONE : ZERO;
            return a - b < 0;
        }

        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right) < 0;
    }

    /** Less than equal */
    @SuppressWarnings("unchecked")
    public static boolean lte(Object left, Object right)
    {
        if (left instanceof Number)
        {
            Number l = (Number) left;
            Number r = convertToNumber(l, right, true);
            if (r != null)
            {
                return cmp(l, r) <= 0;
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return cmp(l, r) <= 0;
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            byte a = ((Boolean) left).booleanValue() ? ONE : ZERO;
            byte b = ((Boolean) right).booleanValue() ? ONE : ZERO;
            return a - b <= 0;
        }

        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right) <= 0;
    }

    /** Compare */
    @SuppressWarnings("unchecked")
    public static int cmp(Object left, Object right)
    {
        if ((left instanceof Double
            || left instanceof BigDecimal)
            && right instanceof Number)
        {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        }
        else if ((right instanceof Double
            || right instanceof BigDecimal)
            && left instanceof Number)
        {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        }
        else if (left instanceof Float
            && right instanceof Number)
        {
            return Float.compare(((Number) left).floatValue(), ((Number) right).floatValue());
        }
        else if (right instanceof Float
            && left instanceof Number)
        {
            return Float.compare(((Number) left).floatValue(), ((Number) right).floatValue());
        }
        else if (left instanceof Long
            && right instanceof Number)
        {
            return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
        }
        else if (right instanceof Long
            && left instanceof Number)
        {
            return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
        }
        else if (left instanceof Integer
            && right instanceof Number)
        {
            return Integer.compare(((Number) left).intValue(), ((Number) right).intValue());
        }
        else if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right);
    }

    /** Equal */
    public static boolean eq(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return left == right;
        }
        return eq(left, right, true);
    }

    /** Equal */
    @SuppressWarnings("unchecked")
    public static boolean eq(Object left, Object right, boolean throwIfNotComparable)
    {
        if (left instanceof Number)
        {
            Number l = (Number) left;
            Number r = convertToNumber(l, right, throwIfNotComparable);
            if (r != null)
            {
                return cmp(l, r) == 0;
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, throwIfNotComparable);
            if (l != null)
            {
                return cmp(l, r) == 0;
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            return ((Boolean) left).booleanValue() == ((Boolean) right).booleanValue();
        }

        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            if (throwIfNotComparable)
            {
                throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
            }

            return false;
        }

        return ((Comparable<Object>) left).compareTo(right) == 0;
    }

    /** Arithmetic operation */
    private static Number arithmetic(
            Number left,
            Number right,
            DoubleOp doubleOp,
            FloatOp floatOp,
            LongOp longOp,
            IntOp intOp)
    {
        if (left instanceof BigDecimal
            || right instanceof BigDecimal
            || left instanceof Double
            || right instanceof Double)
        {
            return doubleOp.apply(left.doubleValue(), right.doubleValue());
        }
        else if (left instanceof Float
            || right instanceof Float)
        {
            return floatOp.apply(left.floatValue(), right.floatValue());
        }
        else if (left instanceof Long
            || right instanceof Long)
        {
            return longOp.apply(left.longValue(), right.longValue());
        }

        return intOp.apply(left.intValue(), right.intValue());
    }

    /** Add */
    public static Object add(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof CharSequence
            || right instanceof CharSequence)
        {
            return String.valueOf(left) + String.valueOf(right);
        }
        else if (left instanceof Number
            && right instanceof Number)
        {
            return arithmetic((Number) left, (Number) right, ADD_DOUBLE, ADD_FLOAT, ADD_LONG, ADD_INT);
        }

        throw new ArithmeticException("Cannot add " + left + " and " + right);
    }

    /** Subtract */
    public static Number subtract(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Number
            && right instanceof Number)
        {
            return arithmetic((Number) left, (Number) right, SUB_DOUBLE, SUB_FLOAT, SUB_LONG, SUB_INT);
        }

        throw new ArithmeticException("Cannot subtract " + left + " and " + right);
    }

    /** Multiply */
    public static Number multiply(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Number
            && right instanceof Number)
        {
            return arithmetic((Number) left, (Number) right, MUL_DOUBLE, MUL_FLOAT, MUL_LONG, MUL_INT);
        }

        throw new ArithmeticException("Cannot multiply " + left + " and " + right);
    }

    /** Divide */
    public static Number divide(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Number
            && right instanceof Number)
        {
            return arithmetic((Number) left, (Number) right, DIV_DOUBLE, DIV_FLOAT, DIV_LONG, DIV_INT);
        }

        throw new ArithmeticException("Cannot divide " + left + " and " + right);
    }

    /** Modulo */
    public static Number modulo(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Number
            && right instanceof Number)
        {
            return arithmetic((Number) left, (Number) right, MOD_DOUBLE, MOD_FLOAT, MOD_LONG, MOD_INT);
        }

        throw new ArithmeticException("Cannot modulo " + left + " and " + right);
    }

    /** Negate */
    public static Object negate(Object value)
    {
        if (value instanceof Double)
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

        throw new IllegalArgumentException("Cannot negate " + value);
    }

    /* MISC METHODS */

    /** Converts an object to a number (if applicable) taking the number type into consideration */
    private static Number convertToNumber(Number number, Object other, boolean throwIfNotConvertible)
    {
        if (other instanceof Number)
        {
            return (Number) other;
        }
        else if (other instanceof Boolean)
        {
            return (Boolean) other ? 1 : 0;
        }

        if (!(other instanceof String))
        {
            return null;
        }

        String string = (String) other;
        try
        {
            if (number instanceof Double)
            {
                return Double.parseDouble(string);
            }
            else if (number instanceof Float)
            {
                return Float.parseFloat(string);
            }
            else if (number instanceof Long)
            {
                return Long.parseLong(string);
            }
            else if (number instanceof Integer)
            {
                return Integer.parseInt(string);
            }
        }
        catch (NumberFormatException e)
        {
            if (throwIfNotConvertible)
            {
                throw new IllegalArgumentException("Cannot convert value " + other + " to datatype " + number.getClass().getSimpleName());
            }
        }

        return null;
    }

    /** In */
    @SuppressWarnings("unchecked")
    public static boolean inValue(Object value, Object arg)
    {
        if (arg instanceof Collection)
        {
            return ((Collection<Object>) arg).contains(value);
        }
        if (arg instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) arg;
            while (it.hasNext())
            {
                if (eq(value, it.next()))
                {
                    return true;
                }
            }
        }
        else if (value instanceof Collection)
        {
            return ((Collection<Object>) value).contains(arg);
        }
        else if (value instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) value;
            while (it.hasNext())
            {
                if (eq(arg, it.next()))
                {
                    return true;
                }
            }
        }
        else if (eq(value, arg, false))
        {
            return true;
        }
        return false;
    }
}
