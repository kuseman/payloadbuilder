package org.kuse.payloadbuilder.core.parser;

import java.util.Collection;
import java.util.Iterator;

/** Math methods used when evaluating expressions */
public final class ExpressionMath
{
    private ExpressionMath()
    {
    }

    private static final byte ONE = 1;
    private static final byte ZERO = 1;

    /** Equals */
    public static boolean eq(Number left, Number right)
    {
        if (left instanceof Double || right instanceof Double)
        {
            return Double.compare(left.doubleValue(), right.doubleValue()) == 0;
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return Float.compare(left.floatValue(), right.floatValue()) == 0;
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() == right.longValue();
        }

        return left.intValue() == right.intValue();
    }

    /** Greater than */
    public static boolean gt(Number left, Number right)
    {
        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() > right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() > right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() > right.longValue();
        }

        return left.intValue() > right.intValue();
    }

    /** Greater than equal */
    public static boolean gte(Number left, Number right)
    {
        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() >= right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() >= right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() >= right.longValue();
        }

        return left.intValue() >= right.intValue();
    }

    /** Less than */
    public static boolean lt(Number left, Number right)
    {
        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() < right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() < right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() < right.longValue();
        }

        return left.intValue() < right.intValue();
    }

    /** Less than equal */
    public static boolean lte(Number left, Number right)
    {
        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() <= right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() <= right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() <= right.longValue();
        }

        return left.intValue() <= right.intValue();
    }

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
                return gt(l, r);
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return gt(l, r);
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
                return gte(l, r);
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return gte(l, r);
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
                return lt(l, r);
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return lt(l, r);
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
                return lte(l, r);
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, true);
            if (l != null)
            {
                return lte(l, r);
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

    /** Equal */
    public static boolean eq(Object left, Object right)
    {
        return eq(left, right, true);
    }

    /** Compare */
    @SuppressWarnings("unchecked")
    public static int cmp(Object left, Object right)
    {
        if (left instanceof Double && right instanceof Number)
        {
            return Double.compare(((Double) left).doubleValue(), ((Number) right).doubleValue());
        }
        else if (left instanceof Float && right instanceof Number)
        {
            return Float.compare(((Float) left).floatValue(), ((Number) right).floatValue());
        }
        else if (left instanceof Long && right instanceof Number)
        {
            return Long.compare(((Long) left).longValue(), ((Number) right).longValue());
        }
        else if (left instanceof Integer && right instanceof Number)
        {
            return Integer.compare(((Integer) left).intValue(), ((Number) right).intValue());
        }
        else if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }

        return ((Comparable<Object>) left).compareTo(right);
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
                return eq(l, r);
            }
        }
        else if (right instanceof Number)
        {
            Number r = (Number) right;
            Number l = convertToNumber(r, left, throwIfNotComparable);
            if (l != null)
            {
                return eq(l, r);
            }
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            return ((Boolean) left).booleanValue() == ((Boolean) right).booleanValue();
        }
        else if (left == null || right == null)
        {
            return left == null && right == null;
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

    /* ARITHMETIC METHODS  */
    /** Add */
    public static long add(long left, long right)
    {
        return left + right;
    };

    /** Add */
    public static double add(long left, double right)
    {
        return left + right;
    };

    /** Add */
    public static double add(double left, long right)
    {
        return left + right;
    };

    /** Add */
    public static double add(double left, double right)
    {
        return left + right;
    };

    /** Add */
    public static Number add(double left, Number right)
    {
        return right != null ? left + right.doubleValue() : null;
    }

    /** Add */
    public static Number add(Number left, double right)
    {
        return left != null ? left.doubleValue() + right : null;
    }

    /** Add */
    public static Number add(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (left instanceof Double)
        {
            return left.doubleValue() + right;
        }
        else if (left instanceof Float)
        {
            return left.floatValue() + right;
        }
        else if (left instanceof Long)
        {
            return left.longValue() + right;
        }
        return left.intValue() + right;
    }

    /** Add */
    public static Number add(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (right instanceof Double)
        {
            return left + right.doubleValue();
        }
        else if (right instanceof Float)
        {
            return left + right.floatValue();
        }
        else if (right instanceof Long)
        {
            return left + right.longValue();
        }
        return left + right.intValue();
    }

    /** Add */
    public static Number add(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() + right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() + right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() + right.longValue();
        }

        return left.intValue() + right.intValue();
    }

    /** Subtract */
    public static long subtract(long left, long right)
    {
        return left - right;
    };

    /** Subtract */
    public static double subtract(long left, double right)
    {
        return left - right;
    };

    /** Subtract */
    public static double subtract(double left, long right)
    {
        return left - right;
    };

    /** Subtract */
    public static double subtract(double left, double right)
    {
        return left - right;
    };

    /** Subtract */
    public static Number subtract(double left, Number right)
    {
        return right != null ? left - right.longValue() : null;
    }

    /** Subtract */
    public static Number subtract(Number left, double right)
    {
        return left != null ? left.longValue() - right : null;
    }

    /** Subtract */
    public static Number subtract(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (left instanceof Double)
        {
            return left.doubleValue() - right;
        }
        else if (left instanceof Float)
        {
            return left.floatValue() - right;
        }
        else if (left instanceof Long)
        {
            return left.longValue() - right;
        }
        return left.intValue() - right;
    }

    /** Subtract */
    public static Number subtract(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (right instanceof Double)
        {
            return left - right.doubleValue();
        }
        else if (right instanceof Float)
        {
            return left - right.floatValue();
        }
        else if (right instanceof Long)
        {
            return left - right.longValue();
        }
        return left - right.intValue();
    }

    /** Subtract */
    public static Number subtract(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() - right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() - right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() - right.longValue();
        }

        return left.intValue() - right.intValue();
    }

    /** Multiply */
    public static long multiply(long left, long right)
    {
        return left * right;
    };

    /** Multiply */
    public static double multiply(long left, double right)
    {
        return left * right;
    };

    /** Multiply */
    public static double multiply(double left, long right)
    {
        return left * right;
    };

    /** Multiply */
    public static double multiply(double left, double right)
    {
        return left * right;
    };

    /** Multiply */
    public static Number multiply(double left, Number right)
    {
        return right != null ? left * right.doubleValue() : null;
    }

    /** Multiply */
    public static Number multiply(Number left, double right)
    {
        return left != null ? left.doubleValue() * right : null;
    }

    /** Multiply */
    public static Number multiply(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (left instanceof Double)
        {
            return left.doubleValue() * right;
        }
        else if (left instanceof Float)
        {
            return left.floatValue() * right;
        }
        else if (left instanceof Long)
        {
            return left.longValue() * right;
        }
        return left.intValue() * right;
    }

    /** Multiply */
    public static Number multiply(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (right instanceof Double)
        {
            return left * right.doubleValue();
        }
        else if (right instanceof Float)
        {
            return left * right.floatValue();
        }
        else if (right instanceof Long)
        {
            return left * right.longValue();
        }
        return left * right.intValue();
    }

    /** Multiply */
    public static Number multiply(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() * right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() * right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() * right.longValue();
        }

        return left.intValue() * right.intValue();
    }

    /** Divide */
    public static long divide(long left, long right)
    {
        return left / right;
    };

    /** Divide */
    public static double divide(long left, double right)
    {
        return left / right;
    };

    /** Divide */
    public static double divide(double left, long right)
    {
        return left / right;
    };

    /** Divide */
    public static double divide(double left, double right)
    {
        return left / right;
    };

    /** Divide */
    public static Number divide(double left, Number right)
    {
        return right != null ? left / right.doubleValue() : null;
    }

    /** Divide */
    public static Number divide(Number left, double right)
    {
        return left != null ? left.doubleValue() / right : null;
    }

    /** Divide */
    public static Number divide(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (left instanceof Double)
        {
            return left.doubleValue() / right;
        }
        else if (left instanceof Float)
        {
            return left.floatValue() / right;
        }
        else if (left instanceof Long)
        {
            return left.longValue() / right;
        }
        return left.intValue() / right;
    }

    /** Divide */
    public static Number divide(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (right instanceof Double)
        {
            return left / right.doubleValue();
        }
        else if (right instanceof Float)
        {
            return left / right.floatValue();
        }
        else if (right instanceof Long)
        {
            return left / right.longValue();
        }
        return left / right.intValue();
    }

    /** Divide */
    public static Number divide(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() / right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() / right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() / right.longValue();
        }

        return left.intValue() / right.intValue();
    }

    /** Modulo */
    public static long modulo(long left, long right)
    {
        return left % right;
    };

    /** Modulo */
    public static double modulo(long left, double right)
    {
        return left % right;
    };

    /** Modulo */
    public static double modulo(double left, long right)
    {
        return left % right;
    };

    /** Modulo */
    public static double modulo(double left, double right)
    {
        return left % right;
    };

    /** Modulo */
    public static Number modulo(double left, Number right)
    {
        return right != null ? left % right.doubleValue() : null;
    }

    /** Modulo */
    public static Number modulo(Number left, double right)
    {
        return left != null ? left.doubleValue() % right : null;
    }

    /** Modulo */
    public static Number modulo(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (left instanceof Double)
        {
            return left.doubleValue() % right;
        }
        else if (left instanceof Float)
        {
            return left.floatValue() % right;
        }
        else if (left instanceof Long)
        {
            return left.longValue() % right;
        }
        return left.intValue() % right;
    }

    /** Modulo */
    public static Number modulo(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (right instanceof Double)
        {
            return left % right.doubleValue();
        }
        else if (right instanceof Float)
        {
            return left % right.floatValue();
        }
        else if (right instanceof Long)
        {
            return left % right.longValue();
        }
        return left % right.intValue();
    }

    /** Modulo */
    public static Number modulo(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (left instanceof Double || right instanceof Double)
        {
            return left.doubleValue() % right.doubleValue();
        }
        else if (left instanceof Float || right instanceof Float)
        {
            return left.floatValue() % right.floatValue();
        }
        else if (left instanceof Long || right instanceof Long)
        {
            return left.longValue() % right.longValue();
        }

        return left.intValue() % right.intValue();
    }

    /** Add */
    public static Object add(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof CharSequence || right instanceof CharSequence)
        {
            return String.valueOf(left) + String.valueOf(right);
        }
        else if (left instanceof Number && right instanceof Number)
        {
            return add((Number) left, (Number) right);
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
        else if (left instanceof Number && right instanceof Number)
        {
            return subtract((Number) left, (Number) right);
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
        else if (left instanceof Number && right instanceof Number)
        {
            return multiply((Number) left, (Number) right);
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
        else if (left instanceof Number && right instanceof Number)
        {
            return divide((Number) left, (Number) right);
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
        else if (left instanceof Number && right instanceof Number)
        {
            return modulo((Number) left, (Number) right);
        }

        throw new ArithmeticException("Cannot modulo " + left + " and " + right);
    }

    /** Negate */
    public static Object negate(Object value)
    {
        if (value instanceof Double)
        {
            return -(Double) value;
        }
        else if (value instanceof Float)
        {
            return -(Float) value;
        }
        else if (value instanceof Long)
        {
            return -(Long) value;
        }
        else if (value instanceof Integer)
        {
            return -(Integer) value;
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
