package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.parser.Expression;

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
    public void extract(ExecutionContext context, Tuple tuple, Object[] values)
    {
        context.getStatementContext()
                .setTuple(tuple);
        for (int i = 0; i < size; i++)
        {
            values[i] = expressions.get(i)
                    .eval(context);
        }
        context.getStatementContext()
                .setTuple(null);
    }

    @Override
    public int size()
    {
        return size;
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