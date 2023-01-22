package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** If statement */
public class IfStatement extends Statement
{
    private final IExpression condition;
    private final List<Statement> statements;
    private final List<Statement> elseStatements;

    public IfStatement(IExpression condition, List<Statement> statements, List<Statement> elseStatements)
    {
        this.condition = requireNonNull(condition, "condition");
        this.statements = requireNonNull(statements, "statements");
        this.elseStatements = requireNonNull(elseStatements, "elseStatements");
    }

    public IExpression getCondition()
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

    @Override
    public List<Statement> getChildren()
    {
        List<Statement> children = new ArrayList<>(statements.size() + elseStatements.size());
        children.addAll(statements);
        children.addAll(elseStatements);
        return children;
    }
}
