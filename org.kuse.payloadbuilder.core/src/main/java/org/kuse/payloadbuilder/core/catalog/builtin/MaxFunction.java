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

/** Max input */
class MaxFunction extends ScalarFunctionInfo
{
    MaxFunction(Catalog catalog)
    {
        super(catalog, "max");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        Object max = null;
        if (obj instanceof Iterator)
        {
            max = max((Iterator<Object>) obj);
        }
        else if (obj instanceof Iterable)
        {
            max = max(((Iterable<Object>) obj).iterator());
        }
        else
        {
            return obj;
        }
        return max;
    }

    private Object max(Iterator<Object> it)
    {
        Object max = null;
        while (it.hasNext())
        {
            if (max == null)
            {
                max = it.next();
            }
            else
            {
                Object val = it.next();
                int r = ExpressionMath.cmp(max, val);
                if (r < 0)
                {
                    max = val;
                }
            }
        }
        return max;
    }
}
