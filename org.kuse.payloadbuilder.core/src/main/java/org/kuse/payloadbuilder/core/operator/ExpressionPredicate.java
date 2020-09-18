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

import java.util.function.BiPredicate;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Predicate that operates over an expression */
class ExpressionPredicate implements BiPredicate<ExecutionContext, Tuple>
{
    private final Expression predicate;

    ExpressionPredicate(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }

    StopWatch sw = new StopWatch();

    @Override
    public boolean test(ExecutionContext context, Tuple tuple)
    {
        sw.start();
        context.setTuple(tuple);
        boolean result = BooleanUtils.isTrue((Boolean) predicate.eval(context));
        sw.stop();
        context.evalTime.addAndGet(sw.getTime());
        sw.reset();

        return result;
    }

    @Override
    public int hashCode()
    {
        return predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionPredicate)
        {
            return predicate.equals(((ExpressionPredicate) obj).predicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return predicate.toString();
    }
}
