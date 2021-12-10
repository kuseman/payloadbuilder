package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Expression based ordinal values factory. */
class ExpressionOrdinalValuesFactory implements IOrdinalValuesFactory
{
    private final List<Expression> expressions;

    ExpressionOrdinalValuesFactory(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
    }

    @Override
    public IOrdinalValues create(ExecutionContext context, Tuple tuple)
    {
        int size = expressions.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++)
        {
            context.getStatementContext().setTuple(tuple);
            values[i] = expressions.get(i).eval(context);
        }
        context.getStatementContext().setTuple(null);
        return new OrdinalValues(values);
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionOrdinalValuesFactory)
        {
            ExpressionOrdinalValuesFactory that = (ExpressionOrdinalValuesFactory) obj;
            return expressions.equals(that.expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expressions.toString();
    }

    /** Ordinal values from Object array */
    static class OrdinalValues implements IOrdinalValues
    {
        private final Object[] values;
        private final int hashCode;

        OrdinalValues(Object[] values)
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
            if (!(obj instanceof IOrdinalValues))
            {
                return false;
            }

            return isEquals((IOrdinalValues) obj);
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
