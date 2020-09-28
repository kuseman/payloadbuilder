package org.kuse.payloadbuilder.core.test;

import java.util.List;

/** Harness table */
public class TestTable
{
    private String name;
    private List<String> columns;
    private List<Object[]> rows;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public void setColumns(List<String> columns)
    {
        this.columns = columns;
    }

    public List<Object[]> getRows()
    {
        return rows;
    }

    public void setRows(List<Object[]> rows)
    {
        this.rows = rows;
    }
}
