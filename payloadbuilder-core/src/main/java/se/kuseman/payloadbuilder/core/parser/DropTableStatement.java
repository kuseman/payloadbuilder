package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Drop table statement */
public class DropTableStatement extends Statement
{
    private final String catalogAlias;
    private final QualifiedName qname;
    private final boolean lenient;
    private final boolean tempTable;
    private final Token token;

    public DropTableStatement(String catalogAlias, QualifiedName qname, boolean lenient, boolean tempTable, Token token)
    {
        this.catalogAlias = catalogAlias;
        this.tempTable = tempTable;
        this.token = token;
        this.qname = requireNonNull(qname, "qname");
        this.lenient = lenient;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public boolean isLenient()
    {
        return lenient;
    }

    public boolean isTempTable()
    {
        return tempTable;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
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
