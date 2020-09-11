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

import java.io.IOException;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/** Turns a json string into object */
class JsonValueFunction extends ScalarFunctionInfo
{
    private static final ObjectReader READER = new ObjectMapper().readerFor(Object.class);

    JsonValueFunction(Catalog catalog)
    {
        super(catalog, "json_value");
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg = arguments.get(0).eval(context);
        if (arg == null)
        {
            return null;
        }

        try
        {
            return READER.readValue(String.valueOf(arg));
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Error deserializing '" + arg + "'", e);
        }
    }
}
