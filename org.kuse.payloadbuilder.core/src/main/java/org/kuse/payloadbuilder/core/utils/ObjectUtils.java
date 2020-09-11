/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.utils;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

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

    /** Checks that provided string is not blank, throws exception otherwise */
    public static String requireNonBlank(String string, String message)
    {
        if (StringUtils.isBlank(string))
        {
            throw new IllegalArgumentException(message);
        }
        return string;
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
