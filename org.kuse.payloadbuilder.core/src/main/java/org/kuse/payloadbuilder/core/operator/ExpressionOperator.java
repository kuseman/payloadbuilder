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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.utils.IteratorUtils;

/** Operator that operates over an expression (that returns rows) */
class ExpressionOperator extends AOperator
{
    private final TableAlias MAP_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of(""), "").asteriskColumns().build();
    private final Expression expression;

    ExpressionOperator(int nodeId, Expression expression)
    {
        super(nodeId);
        this.expression = requireNonNull(expression, "expression");
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowIterator open(ExecutionContext context)
    {
        Object result = expression.eval(context);
        if (result == null)
        {
            return RowIterator.EMPTY;
        }

        // Transformer that supports Map's beyond rows
        Iterator<Object> iterator = IteratorUtils.getIterator(result);
        return new RowIterator()
        {
            private Set<String> columns;
            private String[] rowColumns;
            private int pos;

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            };

            @Override
            public Row next()
            {
                return transform(iterator.next());
            };

            private Row transform(Object input)
            {
                if (input == null)
                {
                    return null;
                }
                else if (input instanceof Row)
                {
                    return (Row) input;
                }
                else if (input instanceof Map)
                {
                    Map<String, Object> map = (Map<String, Object>) input;
                    if (columns == null)
                    {
                        columns = new LinkedHashSet<>(map.keySet());
                        rowColumns = columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
                    }
                    else if (columns.addAll(map.keySet()))
                    {
                        rowColumns = columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
                    }

                    int length = rowColumns.length;
                    Object[] values = new Object[length];
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = map.get(rowColumns[i]);
                    }

                    return Row.of(MAP_ALIAS, pos++, rowColumns, values);
                }

                throw new IllegalArgumentException("Cannot iterate over " + input);
            }
        };
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionOperator)
        {
            ExpressionOperator that = (ExpressionOperator) obj;
            return nodeId == that.nodeId
                && expression.equals(that.expression);
        }
        return false;
    }
}
