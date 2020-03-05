package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class JoinedTableSource
{
    private final TableSource tableSource;
    private final List<JoinItem> joins;
    
    public JoinedTableSource(TableSource tableSource, List<JoinItem> joins)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.joins = requireNonNull(joins, "joins");
    }
    
    public TableSource getTableSource()
    {
        return tableSource;
    }
    
    public List<JoinItem> getJoins()
    {
        return joins;
    }
    
    @Override
    public String toString()
    {
        return tableSource.toString() + System.lineSeparator() + joins.stream().map(j -> j.toString()).collect(joining(System.lineSeparator()));
    }
}
