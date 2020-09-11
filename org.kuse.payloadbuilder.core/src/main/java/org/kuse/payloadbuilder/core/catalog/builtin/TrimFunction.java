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

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Lower and upper function */
class TrimFunction extends ScalarFunctionInfo
{
    private final Type type;

    TrimFunction(Catalog catalog, Type type)
    {
        super(catalog, type.name);
        this.type = type;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + type.descriptiveName + " string value of provided argument." + System.lineSeparator() +
            "If argument is non String the argument is returned as is.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj == null)
        {
            return null;
        }

        String value = String.valueOf(obj);
        switch (type)
        {
            case BOTH:
                return StringUtils.trim(value);
            case LEFT:
                return StringUtils.stripStart(value, null);
            case RIGHT:
                return StringUtils.stripEnd(value, null);
        }

        return null;
    }

    enum Type
    {
        BOTH("trim", "trimmed"),
        LEFT("ltrim", "left trimmed"),
        RIGHT("rtrim", "right trimmed");

        String name;
        String descriptiveName;

        Type(String name, String descriptiveName)
        {
            this.name = name;
            this.descriptiveName = descriptiveName;
        }
    }
}
