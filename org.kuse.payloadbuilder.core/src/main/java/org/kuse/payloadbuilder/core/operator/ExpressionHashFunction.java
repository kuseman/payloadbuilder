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
package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Function that generates a hash from provided expressions */
class ExpressionHashFunction implements ToIntBiFunction<ExecutionContext, Row>
{
    private final List<Expression> expressions;

    ExpressionHashFunction(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }

    @Override
    public int applyAsInt(ExecutionContext context, Row row)
    {
        context.setRow(row);
        int hash = 37;
        for (Expression expression : expressions)
        {
            Object result = expression.eval(context);

            // If value is string and is digits, use the intvalue as
            // hash instead of string to be able to compare ints and strings
            // on left/right side of join
            if (result instanceof String && NumberUtils.isDigits((String) result))
            {
                result = Integer.parseInt((String) result);
            }
            hash += 17 * (result != null ? result.hashCode() : 0);
        }
        return hash;
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionHashFunction)
        {
            return expressions.equals(((ExpressionHashFunction) obj).expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expressions.stream().map(Object::toString).collect(Collectors.joining(", "));
    }
}
