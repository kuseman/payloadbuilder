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
package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionMath;

/** Min input */
class MinFunction extends ScalarFunctionInfo
{
    MinFunction(Catalog catalog)
    {
        super(catalog, "min");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        Object min = null;
        if (obj instanceof Iterator)
        {
            min = min((Iterator<Object>) obj);
        }
        else if (obj instanceof Iterable)
        {
            min = min(((Iterable<Object>) obj).iterator());
        }
        else
        {
            return obj;
        }
        return min;
    }

    private Object min(Iterator<Object> it)
    {
        Object min = null;
        while (it.hasNext())
        {
            if (min == null)
            {
                min = it.next();
            }
            else
            {
                Object val = it.next();
                int r = ExpressionMath.cmp(min, val);
                if (r > 0)
                {
                    min = val;
                }
            }
        }
        return min;
    }
}
