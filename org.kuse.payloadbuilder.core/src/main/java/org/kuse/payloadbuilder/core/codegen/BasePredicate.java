package org.kuse.payloadbuilder.core.codegen;

import java.util.function.Predicate;

import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Base class for predicates */
//CSOFF
public abstract class BasePredicate extends BaseGeneratedClass implements Predicate<ExecutionContext>
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
