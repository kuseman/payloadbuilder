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

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Left and -right pad function */
class PadFunction extends ScalarFunctionInfo
{
    private final boolean left;

    PadFunction(Catalog catalog, boolean left)
    {
        super(catalog, left ? "leftpad" : "rightpad");
        this.left = left;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (left ? "left" : "right") + " padded string of first argument." + System.lineSeparator() +
            "with length of second argument." + System.lineSeparator() +
            "A optional third argument can be supplied for pad string (defaults to single white space). " + System.lineSeparator() +
            "Ex. " + (left ? "left" : "right") + "pad(expression, integerExpression [, expression])" + System.lineSeparator() +
            "NOTE! First argument is converted to a string.";
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

        Object lengthObj = arguments.get(1).eval(context);
        if (!(lengthObj instanceof Integer))
        {
            throw new IllegalArgumentException("Expected an integer expression for second argument of " + getName() + " but got " + lengthObj);
        }
        int length = ((Integer) lengthObj).intValue();

        if (arguments.size() >= 3)
        {
            Object padString = arguments.get(2).eval(context);
            return left
                ? StringUtils.leftPad(value, length, padString != null ? String.valueOf(padString) : " ")
                : StringUtils.rightPad(value, length, padString != null ? String.valueOf(padString) : " ");
        }

        return left
            ? StringUtils.leftPad(value, length)
            : StringUtils.rightPad(value, length);
    }
}
