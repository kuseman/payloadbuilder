package org.kuse.payloadbuilder.core.utils;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.EnumerationIterator;
import org.apache.commons.collections.iterators.ObjectArrayIterator;
import org.apache.commons.collections.iterators.SingletonIterator;

/** Collection utils */
public final class CollectionUtils
{
    private CollectionUtils()
    {
    }

    /** Create set from provided items */
    @SafeVarargs
    public static <T> Set<T> asSet(T... items)
    {
        return new HashSet<>(asList(items));
    }

    /** Get iterator from provided object */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> Iterator<T> getIterator(Object obj)
    {
        if (obj instanceof Iterator)
        {
            return (Iterator) obj;
        }
        else if (obj instanceof Collection)
        {
            if (((Collection) obj).size() == 0)
            {
                return emptyIterator();
            }
            return ((Collection) obj).iterator();
        }
        else if (obj instanceof Iterable)
        {
            Iterable<T> it = (Iterable<T>) obj;
            return it.iterator();
        }
        else if (obj instanceof Object[])
        {
            return new ObjectArrayIterator((Object[]) obj);
        }
        else if (obj instanceof Enumeration)
        {
            return new EnumerationIterator((Enumeration) obj);
        }
        else if (obj != null && obj.getClass().isArray())
        {
            return new ArrayIterator(obj);
        }

        return new SingletonIterator(obj);
    }
}
