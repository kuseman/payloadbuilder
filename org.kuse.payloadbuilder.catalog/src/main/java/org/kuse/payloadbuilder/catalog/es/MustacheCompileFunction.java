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
package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

/** Compile mustable template */
class MustacheCompileFunction extends ScalarFunctionInfo
{
    private static final MustacheFactory FACTORY = new DefaultMustacheFactory();

    MustacheCompileFunction(Catalog catalog)
    {
        super(catalog, "mustachecompile");
    }

    @Override
    public String getDescription()
    {
        return "Compiles provided mustable template with provided params." + System.lineSeparator() +
            "Ex. mustachecompile(templateExpression, paramsExpression)" + System.lineSeparator() +
            "NOTE! paramsExpression should evaluate to a Map.";
    }

    @Override
    public Class<?> getDataType()
    {
        return String.class;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object templateObj = arguments.get(0).eval(context);
        if (!(templateObj instanceof String))
        {
            throw new IllegalArgumentException("Expected template argument for " + getName() + " to return a String, got: " + templateObj);
        }
        Object paramsObj = arguments.get(1).eval(context);
        if (!(paramsObj instanceof Map))
        {
            throw new IllegalArgumentException("Expected params argument for " + getName() + " to return a Map, got: " + paramsObj);
        }
        String template = (String) templateObj;
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) paramsObj;
        Mustache compiledTemplate = FACTORY.compile(new StringReader(template), "template");
        StringWriter writer = new StringWriter();
        compiledTemplate.execute(writer, params);
        return writer.toString();
    }
}
