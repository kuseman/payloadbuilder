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
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns first item that is not a blank string */
class IsBlankFunction extends ScalarFunctionInfo
{
    IsBlankFunction(Catalog catalog)
    {
        super(catalog, "isblank");
    }

    @Override
    public String getDescription()
    {
        return "Returns first non blank value of provided arguments. " + System.lineSeparator() +
            "Ex. isblank(expression1, expression2)" + System.lineSeparator() +
            "If both arguments is blank, second argument is returned. " + System.lineSeparator() +
            "NOTE! First argument is transfomed to a string to determine blank-ness.";
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
        if (obj != null && !isBlank(String.valueOf(obj)))
        {
            return obj;
        }

        return arguments.get(1).eval(context);
    }
}
