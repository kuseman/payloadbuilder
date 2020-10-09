package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Base class for table sources */
//CSOFF
public abstract class TableSource extends ASelectNode
//CSON
{
    protected final TableAlias tableAlias;
    protected final Token token;
    protected final List<Option> options;

    public TableSource(TableAlias tableAlias, List<Option> options, Token token)
    {
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.options = requireNonNull(options, "options");
        this.token = requireNonNull(token, "token");
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    /** Get table options (if applicable) */
    public List<Option> getOptions()
    {
        return options;
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
