package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Expression based index values factory. */
class ExpressionIndexValuesFactory implements IIndexValuesFactory
{
    private final List<Expression> expressions;

    ExpressionIndexValuesFactory(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }

    @Override
    public IIndexValues create(ExecutionContext context, Tuple tuple)
    {
        int size = expressions.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++)
        {
            context.getStatementContext().setTuple(tuple);
            values[i] = expressions.get(i).eval(context);
        }
        context.getStatementContext().setTuple(null);
        return new IndexValues(values);
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionIndexValuesFactory)
        {
            ExpressionIndexValuesFactory that = (ExpressionIndexValuesFactory) obj;
            return expressions.equals(that.expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expressions.toString();
    }

    /** Index values from Object array */
    static class IndexValues implements IIndexValues
    {
        private final Object[] values;
        private final int hashCode;

        IndexValues(Object[] values)
        {
            this.values = values;
            this.hashCode = hash();
        }

        @Override
        public int size()
        {
            return values.length;
        }

        @Override
        public Object getValue(int ordinal)
        {
            return values[ordinal];
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof IIndexValues))
            {
                return false;
            }

            return isEquals((IIndexValues) obj);
        }

        private int hash()
        {
            int hash = ObjectUtils.HASH_CONSTANT;
            int length = values.length;
            for (int i = 0; i < length; i++)
            {
                hash = hash * ObjectUtils.HASH_MULTIPLIER + ObjectUtils.hash(values[i]);
            }
            return hash;
        }
    }
}
