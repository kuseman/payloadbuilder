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

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Distinct input */
class DistinctFunction extends ScalarFunctionInfo
{
    DistinctFunction(Catalog catalog)
    {
        super(catalog, "distinct");
    }

    // Aliases equals the input arguments aliases
    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Expression> arguments, Function<Expression, Set<TableAlias>> aliasResolver)
    {
        return aliasResolver.apply(arguments.get(0));
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        Set<Object> result = null;
        if (obj instanceof Iterator)
        {
            result = new HashSet<>();
            Iterator<Object> it = (Iterator<Object>) obj;
            while (it.hasNext())
            {
                result.add(it.next());
            }
        }
        else if (obj instanceof Set)
        {
            result = (Set<Object>) obj;
        }
        else if (obj instanceof Collection)
        {
            result = new HashSet<>((Collection<Object>) obj);
        }

        return result;
    }
}
