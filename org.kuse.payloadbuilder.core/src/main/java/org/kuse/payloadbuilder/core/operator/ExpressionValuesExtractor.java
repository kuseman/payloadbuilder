package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

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

    StopWatch sw = new StopWatch();

    @Override
    public void extract(ExecutionContext context, Row row, Object[] values)
    {
        sw.start();
        context.setRow(row);
        for (int i = 0; i < size; i++)
        {
            values[i] = expressions.get(i).eval(context);
        }
        sw.stop();
        context.evalTime.addAndGet(sw.getTime());
        sw.reset();
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
