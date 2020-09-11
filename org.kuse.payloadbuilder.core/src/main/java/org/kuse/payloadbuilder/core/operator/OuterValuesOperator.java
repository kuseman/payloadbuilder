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

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Operator used for index access in child operator. Ie.used in FROM operator with index access in where condition
 */
class OuterValuesOperator extends AOperator
{
    private final Operator operator;
    private final List<Expression> valueExpressions;

    OuterValuesOperator(int nodeId, Operator operator, List<Expression> valueExpressions)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.valueExpressions = requireNonNull(valueExpressions, "valueExpressions");
    }

    @Override
    public String getName()
    {
        return "Outer values";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(entry("Values", valueExpressions.toString()));
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowIterator open(ExecutionContext context)
    {
        int size = valueExpressions.size();
        Object[] outerValues = new Object[size];

        for (int i = 0; i < size; i++)
        {
            outerValues[i] = valueExpressions.get(i).eval(context);
        }

        context.getOperatorContext().setOuterIndexValues(new SingletonIterator(outerValues));
        return operator.open(context);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * operator.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof OuterValuesOperator)
        {
            OuterValuesOperator that = (OuterValuesOperator) obj;
            return operator.equals(that.operator)
                && valueExpressions.equals(that.valueExpressions);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("OUTER VALUES (ID: %d, OUTER VALUES: %s", nodeId, valueExpressions);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

}
