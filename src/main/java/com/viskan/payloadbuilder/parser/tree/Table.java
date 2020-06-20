package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** A table */
public class Table extends TableSource
{
    private final QualifiedName table;
    private final List<TableOption> options;

    public Table(QualifiedName table, String alias, List<TableOption> options)
    {
        super(alias);
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
    }
    
    public List<TableOption> getOptions()
    {
        return options;
    }
    
    @Override
    public QualifiedName getTable()
    {
        return table;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return table + " " + alias;
    }
}
