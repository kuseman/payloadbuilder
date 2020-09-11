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

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction(Catalog catalog)
    {
        super(catalog, "isnull");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator() +
            "Ex. isnull(expression, expression)" + System.lineSeparator() +
            "If both arguments yield null, null is returned.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj != null)
        {
            return obj;
        }

        return arguments.get(1).eval(context);
    }
}
