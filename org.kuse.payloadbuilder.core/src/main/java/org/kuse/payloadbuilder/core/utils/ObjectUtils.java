package org.kuse.payloadbuilder.core.utils;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

/** Object utils */
public final class ObjectUtils
{
    public static final int HASH_MULTIPLIER = 37;
    public static final int HASH_CONSTANT = 17;

    private ObjectUtils()
    {
    }

    /** Checks that provided string is not blank, throws exception otherwise */
    public static String requireNonBlank(String string, String message)
    {
        if (StringUtils.isBlank(string))
        {
            throw new IllegalArgumentException(message);
        }
        return string;
    }

    /**
     * Check is first arg is a collection type argument and if then checks if second argument is contained within
     */
    @SuppressWarnings("unchecked")
    public static boolean contains(Object collection, Object value)
    {
        if (collection instanceof Collection)
        {
            return ((Collection<Object>) collection).contains(value);
        }
        else if (collection instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) collection;
            while (it.hasNext())
            {
                Object arg = it.next();
                if (ExpressionMath.eq(arg, value))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /** Get hash value from provided object */
    public static int hash(Object object)
    {
        Object obj = object;
        // If value is string and is digits, use the intvalue as
        // hash instead of string to be able to compare ints and strings
        // on left/right side of join
        if (obj instanceof String && NumberUtils.isDigits((String) obj))
        {
            obj = Integer.parseInt((String) obj);
        }
        return obj != null ? obj.hashCode() : 0;
    }
}
