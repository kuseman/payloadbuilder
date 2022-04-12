package se.kuseman.payloadbuilder.core.codegen;

import java.util.function.Predicate;

import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.parser.Expression;

/** Base class for predicates */
// CSOFF
public abstract class BasePredicate extends BaseGeneratedClass implements Predicate<ExecutionContext>
// CSON
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
