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

import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Intepreter based values extractor */
class ExpressionValuesExtractor implements ValuesExtractor
{
    private final List<Expression> expressions;
    private final int size;

    ExpressionValuesExtractor(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
        this.size = expressions.size();
    }

    StopWatch sw = new StopWatch();

    @Override
    public void extract(ExecutionContext context, Row row, Object[] values)
    {
        sw.start();
        context.setRow(row);
        for (int i = 0; i < size; i++)
        {
            values[i] = expressions.get(i).eval(context);
        }
        sw.stop();
        context.evalTime.addAndGet(sw.getTime());
        sw.reset();
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionValuesExtractor)
        {
            ExpressionValuesExtractor that = (ExpressionValuesExtractor) obj;
            return expressions.equals(that.expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expressions.toString();
    }
}
