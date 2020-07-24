package org.kuse.payloadbuilder.core.utils;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;

public final class ObjectUtils
{
    private ObjectUtils()
    {
    }

    /** Concats provided iterators into a single iterator */
    @SafeVarargs
    public static Iterator<Object> concat(Iterator<Object>... iterators)
    {
        return IteratorUtils.chainedIterator(iterators);
    }

    /** Concat arguments as a string omitting nulls */
    @SuppressWarnings("unchecked")
    public static Object concat(Object... objects)
    {
        // Special case of concating iterators
        if (Arrays.stream(objects).allMatch(a -> a instanceof Iterator || a instanceof Iterable))
        {
            int length = objects.length;
            Iterator<Object>[] iterators = new Iterator[length];
            for (int i = 0; i < length; i++)
            {
                Object o = objects[i];
                if (o instanceof Iterable)
                {
                    iterators[i] = ((Iterable<Object>) o).iterator();
                }
                else
                {
                    iterators[i] = (Iterator<Object>) o;
                }
            }
            return concat(iterators);
        }

        StringBuilder sb = new StringBuilder();
        for (Object object : objects)
        {
            if (object != null)
            {
                sb.append(object);
            }
        }
        return sb.toString();
    }
}
