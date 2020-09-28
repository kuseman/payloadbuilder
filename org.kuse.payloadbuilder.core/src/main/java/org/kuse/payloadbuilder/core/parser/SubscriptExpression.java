package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;

/** Subscript. index acces etc. ie "array[0]" */
public class SubscriptExpression extends Expression
{
    private final Expression value;
    private final Expression subscript;

    public SubscriptExpression(Expression value, Expression subscript)
    {
        this.value = requireNonNull(value, "value");
        this.subscript = requireNonNull(subscript, "subscript");
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getSubscript()
    {
        return subscript;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return value.isNullable() && subscript.isNullable();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Object eval(ExecutionContext context)
    {
        Object value = this.value.eval(context);
        if (value == null)
        {
            return null;
        }

        Object subscript = this.subscript.eval(context);

        if (subscript == null)
        {
            return null;
        }

        if (value instanceof Iterator)
        {
            return getFromIterator((Iterator<Object>) value, subscript);
        }
        else if (value instanceof Iterable)
        {
            return getFromIterator(((Iterable<Object>) value).iterator(), subscript);
        }
        else if (value.getClass().isArray())
        {
            int index = getInt(subscript);
            int length = Array.getLength(value);
            return index >= 0 && index < length ? Array.get(value, index) : null;
        }
        else if (value instanceof Map)
        {
            String key = getString(subscript);
            return ((Map) value).get(key);
        }

        throw new IllegalArgumentException("Cannot subscript value: " + value);
    }

    private Object getFromIterator(Iterator<Object> iterator, Object subscript)
    {
        int index = getInt(subscript);
        int count = 0;
        while (iterator.hasNext())
        {
            Object val = iterator.next();
            if (count == index)
            {
                return val;
            }
            count++;
        }
        return null;
    }

    private int getInt(Object subscript)
    {
        if (!(subscript instanceof Integer))
        {
            throw new IllegalArgumentException("Expected an integer subscript but got " + subscript);
        }

        return ((Integer) subscript).intValue();
    }

    private String getString(Object subscript)
    {
        if (!(subscript instanceof String))
        {
            throw new IllegalArgumentException("Expected a string subscript but got " + subscript);
        }

        return (String) subscript;
    }

    @Override
    public int hashCode()
    {
        return 17
            + (37 * value.hashCode())
            + (37 * subscript.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SubscriptExpression)
        {
            SubscriptExpression that = (SubscriptExpression) obj;
            return value.equals(that.value)
                && subscript.equals(that.subscript);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return value + "[" + subscript + "]";
    }
}
