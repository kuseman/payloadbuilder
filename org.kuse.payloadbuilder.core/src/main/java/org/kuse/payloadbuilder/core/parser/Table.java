package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;

/** A table */
public class Table extends TableSource
{
    private final String catalog;
    private final QualifiedName table;
    private final List<TableOption> options;

    public Table(String catalog, QualifiedName table, String alias, List<TableOption> options, Token token)
    {
        super(alias, token);
        this.catalog = catalog;
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
    }
    
    @Override
    public List<TableOption> getTableOptions()
    {
        return options;
    }
    
    @Override
    public String getCatalog()
    {
        return catalog;
    }
    
    @Override
    public QualifiedName getTable()
    {
        return table;
    }
    
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return table + " " + alias;
    }
}
