package org.kuse.payloadbuilder.core.utils;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;

/** Object utils */
public final class ObjectUtils
{
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
}
