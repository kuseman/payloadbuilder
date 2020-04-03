package com.viskan.payloadbuilder.evaluation;

import java.util.Collection;

/** Math methods used when evaluating expressions */
public final class ExpressionMath
{
    private ExpressionMath()
    {
    }

    private static final byte ONE = 1;
    private static final byte ZERO = 1;

    /* COMPARISON METHODS
     *
     * Comparison is supported between:
     * - numbers
     * - booleans
     * - strings
     **/
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

    @SuppressWarnings("unchecked")
    public static boolean gt(Object left, Object right)
    {
        if (left instanceof Number && right instanceof Number)
        {
            return gt((Number) left, (Number) right);
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

        return ((Comparable) left).compareTo(right) > 0;
    }

    @SuppressWarnings("unchecked")
    public static boolean gte(Object left, Object right)
    {
        if (left instanceof Number && right instanceof Number)
        {
            return gte((Number) left, (Number) right);
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

        return ((Comparable) left).compareTo(right) >= 0;
    }

    @SuppressWarnings("unchecked")
    public static boolean lt(Object left, Object right)
    {
        if (left instanceof Number && right instanceof Number)
        {
            return lt((Number) left, (Number) right);
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

        return ((Comparable) left).compareTo(right) < 0;
    }

    @SuppressWarnings("unchecked")
    public static boolean lte(Object left, Object right)
    {
        if (left instanceof Number && right instanceof Number)
        {
            return lte((Number) left, (Number) right);
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

        return ((Comparable) left).compareTo(right) <= 0;
    }

    public static boolean eq(Object left, Object right)
    {
        return eq(left, right, true);
    }
    
    @SuppressWarnings("unchecked")
    public static boolean eq(Object left, Object right, boolean throwIfNotComparable)
    {
        if (left instanceof Number && right instanceof Number)
        {
            return eq((Number) left, (Number) right);
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

        return ((Comparable) left).compareTo(right) == 0;
    }

    /* ARITHMETIC METHODS  */

    public static long add(long left, long right)
    {
        return left + right;
    };

    public static double add(long left, double right)
    {
        return left + right;
    };

    public static double add(double left, long right)
    {
        return left + right;
    };

    public static double add(double left, double right)
    {
        return left + right;
    };

    public static Number add(double left, Number right)
    {
        return right != null ? left + right.doubleValue() : null;
    }

    public static Number add(Number left, double right)
    {
        return left != null ? left.doubleValue() + right : null;
    }

    public static Number add(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (isDecimal(left))
        {
            return left.doubleValue() + right;
        }
        return left.intValue() + right;
    }

    public static Number add(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (isDecimal(right))
        {
            return left + right.doubleValue();
        }
        return left + right.longValue();
    }

    public static Number add(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (isDecimal(left) || isDecimal(right))
        {
            return left.doubleValue() + right.doubleValue();
        }

        return left.longValue() + right.longValue();
    }

    public static long subtract(long left, long right)
    {
        return left - right;
    };

    public static double subtract(long left, double right)
    {
        return left - right;
    };

    public static double subtract(double left, long right)
    {
        return left - right;
    };

    public static double subtract(double left, double right)
    {
        return left - right;
    };

    public static Number subtract(double left, Number right)
    {
        return right != null ? left - right.longValue() : null;
    }

    public static Number subtract(Number left, double right)
    {
        return left != null ? left.longValue() - right : null;
    }

    public static Number subtract(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (isDecimal(left))
        {
            return left.doubleValue() - right;
        }
        return left.intValue() - right;
    }

    public static Number subtract(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (isDecimal(right))
        {
            return left - right.doubleValue();
        }
        return left - right.longValue();
    }

    public static Number subtract(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (isDecimal(left) || isDecimal(right))
        {
            return left.doubleValue() - right.doubleValue();
        }

        return left.longValue() - right.longValue();
    }

    public static long multiply(long left, long right)
    {
        return left * right;
    };

    public static double multiply(long left, double right)
    {
        return left * right;
    };

    public static double multiply(double left, long right)
    {
        return left * right;
    };

    public static double multiply(double left, double right)
    {
        return left * right;
    };

    public static Number multiply(double left, Number right)
    {
        return right != null ? left * right.doubleValue() : null;
    }

    public static Number multiply(Number left, double right)
    {
        return left != null ? left.doubleValue() * right : null;
    }

    public static Number multiply(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (isDecimal(left))
        {
            return left.doubleValue() * right;
        }
        return left.intValue() * right;
    }

    public static Number multiply(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (isDecimal(right))
        {
            return left * right.doubleValue();
        }
        return left * right.longValue();
    }

    public static Number multiply(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (isDecimal(left) || isDecimal(right))
        {
            return left.doubleValue() * right.doubleValue();
        }

        return left.longValue() * right.longValue();
    }

    public static long divide(long left, long right)
    {
        return left / right;
    };

    public static double divide(long left, double right)
    {
        return left / right;
    };

    public static double divide(double left, long right)
    {
        return left / right;
    };

    public static double divide(double left, double right)
    {
        return left / right;
    };

    public static Number divide(double left, Number right)
    {
        return right != null ? left / right.doubleValue() : null;
    }

    public static Number divide(Number left, double right)
    {
        return left != null ? left.doubleValue() / right : null;
    }

    public static Number divide(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (isDecimal(left))
        {
            return left.doubleValue() / right;
        }
        return left.intValue() / right;
    }

    public static Number divide(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (isDecimal(right))
        {
            return left / right.doubleValue();
        }
        return left / right.longValue();
    }

    public static Number divide(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (isDecimal(left) || isDecimal(right))
        {
            return left.doubleValue() / right.doubleValue();
        }

        return left.longValue() / right.longValue();
    }

    public static long modulo(long left, long right)
    {
        return left % right;
    };

    public static double modulo(long left, double right)
    {
        return left % right;
    };

    public static double modulo(double left, long right)
    {
        return left % right;
    };

    public static double modulo(double left, double right)
    {
        return left % right;
    };

    public static Number modulo(double left, Number right)
    {
        return right != null ? left % right.doubleValue() : null;
    }

    public static Number modulo(Number left, double right)
    {
        return left != null ? left.doubleValue() % right : null;
    }

    public static Number modulo(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if (isDecimal(left))
        {
            return left.doubleValue() % right;
        }
        return left.intValue() % right;
    }

    public static Number modulo(long left, Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if (isDecimal(right))
        {
            return left % right.doubleValue();
        }
        return left % right.longValue();
    }

    public static Number modulo(Number left, Number right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        if (isDecimal(left) || isDecimal(right))
        {
            return left.doubleValue() % right.doubleValue();
        }

        return left.longValue() % right.longValue();
    }

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

    private static boolean isDecimal(Number number)
    {
        return number instanceof Double || number instanceof Float;
    }

    public static boolean inValue(Object value, Object arg)
    {
        if (arg instanceof Collection)
        {
            if (((Collection) arg).contains(value))
            {
                return true;
            }
        }
        else if (eq(value, arg, false))
        {
            return true;
        }
        return false;
    }
}