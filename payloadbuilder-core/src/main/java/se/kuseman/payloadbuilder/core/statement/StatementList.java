package se.kuseman.payloadbuilder.core.statement;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;

/** A container of other statements */
public class StatementList extends Statement
{
    private final List<Statement> statements;

    public StatementList(List<Statement> statements)
    {
        this.statements = unmodifiableList(requireNonNull(statements, "statements"));
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
