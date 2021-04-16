package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Data that is originated from an insert into a temp table. A tuple along with it's columns and values */
public class TemporaryTable
{
    private final QualifiedName name;
    private final TableAlias tableAlias;
    private final String[] columns;
    private final List<Row> rows;

    public TemporaryTable(QualifiedName name, TableAlias tableAlias, String[] columns, List<Row> rows)
    {
        this.name = requireNonNull(name, "name");
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.columns = requireNonNull(columns, "columns");
        this.rows = requireNonNull(rows, "rows");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    public String[] getColumns()
    {
        return columns;
    }

    public List<Row> getRows()
    {
        return rows;
    }

    /** Temporary table row */
    public static class Row
    {
        private final Tuple tuple;
        private final Object[] values;

        public Row(Tuple tuple, Object[] values)
        {
            this.tuple = requireNonNull(tuple, "tuple");
            this.values = requireNonNull(values, "values");
        }

        public Tuple getTuple()
        {
            return tuple;
        }

        public Object[] getValues()
        {
            return values;
        }
    }
}
