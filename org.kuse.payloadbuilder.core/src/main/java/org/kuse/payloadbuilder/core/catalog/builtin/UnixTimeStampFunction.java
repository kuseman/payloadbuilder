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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** unix_timestamp. Date to epuch millis */
class UnixTimeStampFunction extends ScalarFunctionInfo
{
    public UnixTimeStampFunction(Catalog catalog)
    {
        super(catalog, "unix_timestamp");
    }

    @Override
    public Class<?> getDataType()
    {
        return Long.class;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg = arguments.get(0).eval(context);
        if (arg == null)
        {
            return null;
        }
        if (arg instanceof LocalDateTime)
        {
            Instant instant = ((LocalDateTime) arg).atZone(ZoneId.systemDefault()).toInstant();
            return instant.toEpochMilli();
        }
        else if (arg instanceof ZonedDateTime)
        {
            ZonedDateTime zonedDateTime = (ZonedDateTime) arg;
            return zonedDateTime.toInstant().toEpochMilli();
        }

        throw new IllegalArgumentException("Expected a Date to " + getName() + " but got: " + arg);
    }
}
