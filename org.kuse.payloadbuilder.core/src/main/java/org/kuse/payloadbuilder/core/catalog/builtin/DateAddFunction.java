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
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** DateAdd */
class DateAddFunction extends ScalarFunctionInfo
{
    DateAddFunction(Catalog catalog)
    {
        super(catalog, "dateadd");
    }

    @Override
    public String getDescription()
    {
        return "Adds a number for a specific date part to provided Date. " + System.lineSeparator() +
            "Valid parts are: " + System.lineSeparator() +
            Arrays.stream(DatePartFunction.Part.values())
                    .filter(p -> p.abbreviationFor == null)
                    .map(p ->
                    {
                        String name = p.name();
                        List<DatePartFunction.Part> abbreviations = Arrays.stream(DatePartFunction.Part.values()).filter(pp -> pp.abbreviationFor == p).collect(toList());
                        if (abbreviations.isEmpty())
                        {
                            return name;
                        }

                        return name + " ( Abbreviations: " + abbreviations.toString() + " )";
                    })
                    .collect(joining(System.lineSeparator()))
            + System.lineSeparator() +
            "Ex. dateadd(datepartExpression, integerExpression, dateExpression) ";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object value = arguments.get(2).eval(context);
        if (value == null)
        {
            return null;
        }

        String partString;
        Expression partExpression = arguments.get(0);
        if (partExpression instanceof QualifiedReferenceExpression)
        {
            partString = ((QualifiedReferenceExpression) partExpression).getQname().toString();
        }
        else
        {
            Object obj = partExpression.eval(context);
            if (obj == null)
            {
                return null;
            }
            partString = String.valueOf(obj);
        }

        Object numberObj = arguments.get(1).eval(context);
        if (!(numberObj instanceof Integer))
        {
            throw new IllegalArgumentException("Expected a integer expression for " + getName() + " but got: " + numberObj);
        }
        int number = ((Integer) numberObj).intValue();
        if (!(value instanceof Temporal))
        {
            throw new IllegalArgumentException("Expected a valid datetime value for " + getName() + " but got: " + value);
        }

        Temporal temporal = (Temporal) value;
        DatePartFunction.Part part = DatePartFunction.Part.valueOf(partString.toUpperCase());
        return temporal.plus(number, part.getChronoField().getBaseUnit());
    }
}
