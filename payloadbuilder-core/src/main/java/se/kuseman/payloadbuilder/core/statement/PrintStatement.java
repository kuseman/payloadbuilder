package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Print statement */
public class PrintStatement extends Statement
{
    private final IExpression expression;

    public PrintStatement(IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
