package se.kuseman.payloadbuilder.core.codegen;

import java.util.List;
import java.util.function.ToIntBiFunction;

import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.parser.Expression;

/** Base class for generated functions */
// CSOFF
public abstract class BaseHashFunction extends BaseGeneratedClass implements ToIntBiFunction<ExecutionContext, Tuple>
// CSON
{
    private List<Expression> expressions;

    void setExpressions(List<Expression> expressions)
    {
        this.expressions = expressions;
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    @Override
    public String toString()
    {
        return "Gen: " + expressions.toString();
    }
}
