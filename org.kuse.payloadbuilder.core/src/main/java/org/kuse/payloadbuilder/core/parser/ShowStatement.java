package org.kuse.payloadbuilder.core.parser;

import org.antlr.v4.runtime.Token;

/** Show statement for querying current parameters/variables */
public class ShowStatement extends Statement
{
    private final Type type;
    private final String catalog;
    private final Token token;
    
    ShowStatement(Type type, String catalog, Token token)
    {
        this.type = type;
        this.catalog = catalog;
        this.token = token;
    }
    
    public Type getType()
    {
        return type;
    }
    
    public String getCatalog()
    {
        return catalog;
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
    
    public enum Type
    {
        VARIABLES,
        TABLES,
        FUNCTIONS
    }
}
