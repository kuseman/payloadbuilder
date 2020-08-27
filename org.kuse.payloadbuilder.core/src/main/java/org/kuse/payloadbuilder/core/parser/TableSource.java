package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.catalog.TableAlias;

/** Base class for table sources */
public abstract class TableSource extends ASelectNode
{
//    protected final String alias;
    protected final Token token;
    protected final TableAlias tableAlias;
    
    public TableSource(TableAlias tableAlias/*, String alias*/, Token token)
    {
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
//        this.alias = requireNonNull(alias, "alias");
        this.token = requireNonNull(token, "token");
    }
    
    public TableAlias getTableAlias()
    {
        return tableAlias;
    }
    
//    public String getAlias()
//    {
//        return alias;
//    }
    
    /** Get table options (if applicable) */
    public List<Option> getOptions()
    {
        return emptyList();
    }
    
    /** Return catalog (if applicable) for this table source */
    public String getCatalogAlias()
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
