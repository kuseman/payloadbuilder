package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/** Operator that operates over an expression (that returns rows) */
class ExpressionOperator extends AOperator
{
    private static final TableAlias MAP_ALIAS = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of(""), "").asteriskColumns().build();
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
        Iterator<Object> iterator = CollectionUtils.getIterator(result);
        //CSOFF
        return new RowIterator()
        //CSON
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
        return expression.hashCode();
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
