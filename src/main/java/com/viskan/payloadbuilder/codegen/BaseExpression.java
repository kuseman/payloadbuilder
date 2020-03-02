package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.IntPredicate;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
//CSOFF
//@formatter:off
/**
 * Base class for queries.
 * Used by generated code as base class to access helper math methods
 */
public abstract class BaseExpression
{
    static class Input
    {
        private final String column;
        private int ordinal = -2;
        
        public Input(String column)
        {
            this.column = requireNonNull(column);
        }
        
        public Object getObject(Row row)
        {
            if (ordinal == -2)
            {
                ordinal = ArrayUtils.indexOf(row.getTableAlias().getColumns(), column);
            }
            
            return row.getObject(ordinal);
        }
        
        public Number getNumber(Row row)
        {
            return (Number) getObject(row);
        }
        
        public Boolean getBoolean(Row row)
        {
            return (Boolean) getObject(row);
        }
    }
    
    public static IntPredicate EQUAL = c -> c == 0;
    public static IntPredicate NOT_EQUAL = c -> c != 0;
    public static IntPredicate LESS_THAN = c -> c < 0;
    public static IntPredicate LESS_THAN_EQUAL = c -> c <= 0;
    public static IntPredicate GREATER_THAN = c -> c > 0;
    public static IntPredicate GREATER_THAN_EQUAL = c -> c >= 0;
    
    public static boolean compare(long left,    long right,    IntPredicate function)   {return function.test(Long.compare(left, right));}
    public static boolean compare(long left,    double right,  IntPredicate function)   {return function.test(Double.compare(left, right));}
    public static boolean compare(double left,  double right,  IntPredicate function)   {return function.test(Double.compare(left, right));}
    public static boolean compare(double left,  long right,    IntPredicate function)   {return function.test(Double.compare(left, right));}
    public static boolean compare(boolean left, boolean right, IntPredicate function)   {return function.test(Boolean.compare(left, right));}
    public static Boolean compare(double left,  Number right,  IntPredicate function)   {return right != null ? function.test(Double.compare(left, right.doubleValue())) : null;}
    public static Boolean compare(Number left,  double right,  IntPredicate function)   {return left != null ? function.test(Double.compare(left.doubleValue(), right)) : null;}
    public static Boolean compare(long left,    Number right,  IntPredicate function)
    {
        if (right == null)
        {
            return null;
        }
        
        if (isDecimal(right))
        {
            return function.test(Double.compare(left, right.doubleValue()));
        }
        
        return function.test(Long.compare(left, right.longValue()));
    }
    public static Boolean compare(Number left,  long right,     IntPredicate function)
    {
        if (left == null)
        {
            return null;
        }
        
        if (isDecimal(left))
        {
            return function.test(Double.compare(left.doubleValue(), right));
        }
        
        return function.test(Long.compare(left.longValue(), right));
    }
    public static Boolean compare(Number left,  Number right,  IntPredicate function)
    {
        if (left == null || right == null)
        {
            return null;
        }
        
        if (isDecimal(left) || isDecimal(right))
        {
            return function.test(Double.compare(left.doubleValue(), right.doubleValue()));
        }
        
        return function.test(Long.compare(left.longValue(), right.longValue()));
    }

    public static long   add(long left,   long right)   {return left + right;};
    public static double add(long left,   double right) {return left + right;};
    public static double add(double left, long right)   {return left + right;};
    public static double add(double left, double right) {return left + right;};
    public static Number add(double left, Number right) {return right != null ?  left + right.doubleValue() : null;}
    public static Number add(Number left, double right) {return left != null ? left.doubleValue() + right : null;}
    public static Number add(Number left, long right)
    {
        if (left == null)
        {
            return left;
        }
        else if(isDecimal(left))
        {
            return left.doubleValue() + right;
        }
        return left.intValue() + right;
    }
    public static Number add(long left,   Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if(isDecimal(right))
        {
            return left + right.doubleValue();
        }
        return left + right.longValue();
    }  
    public static Number add(Number left,   Number right)
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
    
    public static long    subtract(long left,   long right)   {return left - right;};
    public static double  subtract(long left,   double right) {return left - right;};
    public static double  subtract(double left, long right)   {return left - right;};
    public static double  subtract(double left, double right) {return left - right;};
    public static Number  subtract(double left, Number right) {return right != null ? left - right.longValue() : null;}
    public static Number  subtract(Number left, double right) {return left != null ? left.longValue() - right : null;}
    public static Number  subtract(Number left, long right)   
    {
        if (left == null)
        {
            return left;
        }
        else if(isDecimal(left))
        {
            return left.doubleValue() - right;
        }
        return left.intValue() - right;
    }
    public static Number  subtract(long left,   Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if(isDecimal(right))
        {
            return left - right.doubleValue();
        }
        return left - right.longValue();
    }
    public static Number subtract(Number left,   Number right)
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

    public static long   multiply(long left,   long right)    {return left * right;};
    public static double multiply(long left,   double right) {return left * right;};
    public static double multiply(double left, long right)    {return left * right;};
    public static double multiply(double left, double right) {return left * right;};
    public static Number multiply(double left, Number right) {return right != null ? left * right.doubleValue() : null;}
    public static Number multiply(Number left, double right) {return left != null ? left.doubleValue() * right : null;}
    public static Number multiply(Number left, long right)   
    {
        if (left == null)
        {
            return left;
        }
        else if(isDecimal(left))
        {
            return left.doubleValue() * right;
        }
        return left.intValue() * right;
    }
    public static Number multiply(long left,   Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if(isDecimal(right))
        {
            return left * right.doubleValue();
        }
        return left * right.longValue();
    }
    public static Number multiply(Number left,   Number right)
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
    
    public static long   divide(long left,   long right)    {return left / right;};
    public static double divide(long left,   double right) {return left / right;};
    public static double divide(double left, long right)    {return left / right;};
    public static double divide(double left, double right) {return left / right;};
    public static Number divide(double left, Number right) {return right != null ? left / right.doubleValue() : null;}
    public static Number divide(Number left, double right) {return left != null ? left.doubleValue() / right : null;}
    public static Number divide(Number left, long right)   
    {
        if (left == null)
        {
            return left;
        }
        else if(isDecimal(left))
        {
            return left.doubleValue() / right;
        }
        return left.intValue() / right;
    }
    public static Number divide(long left,   Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if(isDecimal(right))
        {
            return left / right.doubleValue();
        }
        return left / right.longValue();
    }
    public static Number divide(Number left,   Number right)
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
    
    public static long   modulo(long left,  long right)    {return left % right;};
    public static double modulo(long left,   double right) {return left % right;};
    public static double modulo(double left, long right)    {return left % right;};
    public static double modulo(double left, double right) {return left % right;};
    public static Number modulo(double left, Number right) {return right != null ? left % right.doubleValue() : null;}
    public static Number modulo(Number left, double right) {return left != null ? left.doubleValue() % right : null;}
    public static Number modulo(Number left, long right)   
    {
        if (left == null)
        {
            return left;
        }
        else if(isDecimal(left))
        {
            return left.doubleValue() % right;
        }
        return left.intValue() % right;
    }
    public static Number modulo(long left,   Number right)
    {
        if (right == null)
        {
            return right;
        }
        else if(isDecimal(right))
        {
            return left % right.doubleValue();
        }
        return left % right.longValue();
    }
    public static Number modulo(Number left,   Number right)
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
        else if (value instanceof Number && arg instanceof Number)
        {
            Number a = (Number) value;
            Number b = (Number) arg;
            
            if (isDecimal(a) || isDecimal(b))
            {
                return a.doubleValue() == b.doubleValue();
            }
            
            return a.longValue() == b.longValue();
        }
        else if (Objects.equals(value, arg))
        {
            return true;
        }
        return false;
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
////        else if (left instanceof Double && right instanceof Number)
////        {
////            return (Double) left + ((Number) right).doubleValue();
////        }
////        else if (right instanceof Double && left instanceof Number)
////        {
////            return (Double) right + ((Number) left).doubleValue();
////        }
////        else if(left instanceof Long && right instanceof Number)
////        {
////            return (Long) left + ((Number) right).longValue();
////        }
////        else if(right instanceof Long && left instanceof Number)
////        {
////            return (Long) right + ((Number) left).longValue();
////        }
//        
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
    
    @SuppressWarnings("unchecked")
    public static Boolean compare(Object left, Object right, IntPredicate function)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Number && right instanceof Number)
        {
            return compare((Number) left, (Number) right, function);
        }
        
        if (left.getClass() != right.getClass() || !(left instanceof Comparable))
        {
            throw new IllegalArgumentException("Cannot compare " + left + " and " + right);
        }
        
        return function.test(((Comparable) left).compareTo(right));
    }
    
    public static Boolean and(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            return and(((Boolean) left).booleanValue(), ((Boolean) right).booleanValue());
        }
        
        throw new IllegalArgumentException("Cannot and " + left + " and " + right);
    }
    
    public static Boolean or(Object left, Object right)
    {
        if (left == null || right == null)
        {
            return null;
        }
        else if (left instanceof Boolean && right instanceof Boolean)
        {
            return and(((Boolean) left).booleanValue(), ((Boolean) right).booleanValue());
        }
        
        throw new IllegalArgumentException("Cannot or " + left + " and " + right);
    }
    
    public static Object negate(Object value)
    {
        if (value instanceof Double)
        {
            return -(Double) value;
        }
        else if (value instanceof Long)
        {
            return -(Long) value;
        }
        
        throw new IllegalArgumentException("Cannot negate " + value);
    }
    
    /** Returns iterator from input arg */
    public static Iterator getIterator(Object arg)
    {
        return IteratorUtils.getIterator(arg);
    }
}
