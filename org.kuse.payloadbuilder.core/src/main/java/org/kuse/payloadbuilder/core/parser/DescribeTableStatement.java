package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;

/** DESCRIBE table */
public class DescribeTableStatement extends Statement
{
    private final String catalog;
    private final QualifiedName tableName;
    private final Token token;

    DescribeTableStatement(String catalog, QualifiedName tableName, Token token)
    {
        this.catalog = catalog;
        this.tableName = requireNonNull(tableName, "tableName");
        this.token = token;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
