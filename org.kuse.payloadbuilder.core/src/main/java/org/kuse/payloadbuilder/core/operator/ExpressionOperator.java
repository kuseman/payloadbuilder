package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.IteratorUtils;

/** Operator that operates over an expression (that returns rows) */
class ExpressionOperator extends AOperator
{
    private final Expression expression;

    ExpressionOperator(int nodeId, Expression expression)
    {
        super(nodeId);
        this.expression = requireNonNull(expression, "expression");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        Object result = expression.eval(context);
        if (result == null)
        {
            return emptyIterator();
        }

        // Transformer that supports Map's beyond rows
        return new TransformIterator(IteratorUtils.getIterator(result), new Transformer()
        {
            private TableAlias alias;
            private Set<String> columns;
            private int pos;

            @Override
            public Object transform(Object input)
            {
                if (input instanceof Row)
                {
                    return input;
                }
                else if (input instanceof Map)
                {
                    Map<String, Object> map = (Map<String, Object>) input;
                    if (alias == null)
                    {
                        columns = new LinkedHashSet<>(map.keySet());
                        alias = TableAlias.of(null, "", "");
                        alias.setColumns(columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                    }
                    else if (columns.addAll(map.keySet()))
                    {
                        alias.setColumns(columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                    }

                    Object[] values = new Object[columns.size()];
                    int index = 0;
                    for (String column : columns)
                    {
                        values[index++] = map.get(column);
                    }

                    return Row.of(alias, pos++, values);
                }

                throw new IllegalArgumentException("Cannot iterate over " + input);
            }
        });
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