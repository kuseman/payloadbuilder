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
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator that wraps a sub operator and decorates it's
 * tuples with a subquery tuple */
class SubQueryOperator extends AOperator
{
    private final Operator operator;
    private final String alias;
    
    SubQueryOperator(Operator operator, String alias)
    {
        super(operator.getNodeId());
        this.operator = requireNonNull(operator, "operator");
        this.alias = requireNonNull(alias, "alias");
    }
    
    @Override
    public List<Operator> getChildOperators()
    {
        return operator.getChildOperators();
    }

    @Override
    public String getName()
    {
        return operator.getName();
    }

    @Override
    public String getDescribeString()
    {
        return operator.getDescribeString();
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return operator.getDescribeProperties();
    }
    
    @Override
    public RowIterator open(ExecutionContext context)
    {
        final RowIterator it = operator.open(context);
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                return new SubQueryTuple(it.next(), alias);
            }
            
            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }
        };
    }
    
    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SubQueryOperator)
        {
            SubQueryOperator that = (SubQueryOperator) obj;
            return operator.equals(that.operator)
                    && alias.equals(that.alias);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return operator.toString();
    }
    
    @Override
    public String toString(int indent)
    {
        return operator.toString(indent);
    }
}
