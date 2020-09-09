package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** If statement */
public class IfStatement extends Statement
{
    private final Expression condition;
    private final List<Statement> statements;
    private final List<Statement> elseStatements;

    IfStatement(Expression condition, List<Statement> statements, List<Statement> elseStatements)
    {
        this.condition = requireNonNull(condition, "condition");
        this.statements = requireNonNull(statements, "statements");
        this.elseStatements = requireNonNull(elseStatements, "elseStatements");
    }

    public Expression getCondition()
    {
        return condition;
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    public List<Statement> getElseStatements()
    {
        return elseStatements;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
