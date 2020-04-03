package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

/** A table */
public class Table extends TableSource
{
    private final QualifiedName table;

    public Table(QualifiedName table, String alias)
    {
        super(alias);
        this.table = requireNonNull(table, "table");
    }
    
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
