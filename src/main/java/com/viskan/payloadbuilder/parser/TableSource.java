package com.viskan.payloadbuilder.parser;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.NotImplementedException;

/** Base class for table sources */
public abstract class TableSource extends ASelectNode
{
    protected final String alias;
    protected final Token token;
    
    public TableSource(String alias, Token token)
    {
        this.alias = requireNonNull(alias, "alias");
        this.token = requireNonNull(token, "token");
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    /** Get table options (if applicable) */
    public List<TableOption> getTableOptions()
    {
        return emptyList();
    }
    
    /** Return catalog (if applicable) for this table source */
    public String getCatalog()
    {
        return null;
    }
    
    /** Return table (if applicable) for this table source */
    public QualifiedName getTable()
    {
        return null;
    }
    
    public Token getToken()
    {
        return token;
    }
    
    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }

}
