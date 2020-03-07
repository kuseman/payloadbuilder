package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class JoinedTableSource extends ANode
{
    private final TableSource tableSource;
    private final List<AJoin> joins;
    
    public JoinedTableSource(TableSource tableSource, List<AJoin> joins)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.joins = requireNonNull(joins, "joins");
    }
    
    public TableSource getTableSource()
    {
        return tableSource;
    }
    
    public List<AJoin> getJoins()
    {
        return joins;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return tableSource.toString() + (!joins.isEmpty() ? System.lineSeparator() + joins.stream().map(j -> j.toString()).collect(joining(System.lineSeparator())) : "");
    }
}
