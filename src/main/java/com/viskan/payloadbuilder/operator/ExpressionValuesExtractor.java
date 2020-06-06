package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Intepreter based values extractor */
public class ExpressionValuesExtractor implements ValuesExtractor
{
    private final List<Expression> expressions;
    private final int size;

    public ExpressionValuesExtractor(List<Expression> expressions)
    {
        this.expressions = requireNonNull(expressions, "expressions");
        this.size = expressions.size();
    }

    @Override
    public void extract(EvaluationContext context, Row row, Object[] values)
    {
        for (int i = 0; i < size; i++)
        {
            values[i] = expressions.get(i).eval(context, row);
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
