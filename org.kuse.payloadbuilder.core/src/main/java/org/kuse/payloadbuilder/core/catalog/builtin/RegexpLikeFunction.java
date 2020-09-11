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

import java.util.List;
import java.util.regex.Pattern;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Regexp like. Matches input with a regular expression */
class RegexpLikeFunction extends ScalarFunctionInfo
{
    RegexpLikeFunction(Catalog catalog)
    {
        super(catalog, "regexp_like");
    }

    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }

    @Override
    public String getDescription()
    {
        return "Matches first argument to regex provided in second argument." + System.lineSeparator() +
            "Ex. regexp_like(expression, stringExpression)." + System.lineSeparator() +
            "Returns a boolean value.";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);

        if (obj == null)
        {
            return null;
        }

        Object patternObj = arguments.get(1).eval(context);
        if (!(patternObj instanceof String))
        {
            throw new IllegalArgumentException("Expected a String pattern for function " + getName() + " but got " + patternObj);
        }

        String value = String.valueOf(obj);
        Pattern pattern = Pattern.compile((String) patternObj);

        return pattern.matcher(value).find();
    }
}
