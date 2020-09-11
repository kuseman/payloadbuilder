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

import java.time.ZoneOffset;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns current date */
class GetDateFunction extends ScalarFunctionInfo
{
    private final boolean utc;

    GetDateFunction(Catalog catalog, boolean utc)
    {
        super(catalog, utc ? "getutcdate" : "getdate");
        this.utc = utc;
    }

    @Override
    public String getDescription()
    {
        return "Returns current " + (utc ? "UTC " : "") + " Date. " + System.lineSeparator() +
            "NOTE! That same value is used during the whole execution.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList();
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        if (utc)
        {
            return context.getNow().withZoneSameInstant(ZoneOffset.UTC);
        }

        return context.getNow().toLocalDateTime();
    }
}
