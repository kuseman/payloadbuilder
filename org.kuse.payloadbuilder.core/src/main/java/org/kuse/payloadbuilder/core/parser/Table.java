package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.catalog.TableAlias;

/** A table */
public class Table extends TableSource
{
    private final String catalogAlias;
//    private final QualifiedName table;
    private final List<Option> options;

    public Table(String catalogAlias, TableAlias tableAlias/*, QualifiedName table, String alias*/, List<Option> options, Token token)
    {
        super(tableAlias/*, alias*/, token);
        this.catalogAlias = catalogAlias;
//        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
    }
    
    @Override
    public List<Option> getOptions()
    {
        return options;
    }
    
    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }
    
    @Override
    public QualifiedName getTable()
    {
        return tableAlias.getTable();
    }
    
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return tableAlias.toString();
    }
}
