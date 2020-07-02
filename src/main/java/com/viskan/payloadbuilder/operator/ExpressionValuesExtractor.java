package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

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

    @Override
    public void extract(ExecutionContext context, Row row, Object[] values)
    {
        context.setRow(row);
        for (int i = 0; i < size; i++)
        {
            values[i] = expressions.get(i).eval(context);
        }
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
