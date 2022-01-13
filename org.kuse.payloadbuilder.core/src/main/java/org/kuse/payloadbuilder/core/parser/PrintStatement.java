package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Print statement */
public class PrintStatement extends Statement
{
    private final Expression expression;

    public PrintStatement(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
