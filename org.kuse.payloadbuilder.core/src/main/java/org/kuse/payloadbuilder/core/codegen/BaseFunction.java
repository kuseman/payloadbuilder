package org.kuse.payloadbuilder.core.codegen;

import java.util.function.Function;

import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Base class for generated functions */
//CSOFF
public abstract class BaseFunction extends BaseGeneratedClass implements Function<ExecutionContext, Object>
//CSON
{
    private Expression expression;

    void setExpression(Expression expression)
    {
        this.expression = expression;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return "Gen: " + expression.toString();
    }
}
