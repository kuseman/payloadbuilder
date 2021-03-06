package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Top query statement */
public class QueryStatement
{
    private final List<Statement> statements;

    QueryStatement(List<Statement> statements)
    {
        this.statements = requireNonNull(statements, "statements");
    }

    public List<Statement> getStatements()
    {
        return statements;
    }
}
