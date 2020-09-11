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

import static java.util.Collections.emptyIterator;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.EnumerationIterator;
import org.apache.commons.collections.iterators.ObjectArrayIterator;
import org.apache.commons.collections.iterators.SingletonIterator;

public class IteratorUtils
{
    private IteratorUtils()
    {
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
