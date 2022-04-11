package org.kuse.payloadbuilder.core.catalog;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * A table index. Defines columns that can be utilized in batching operators for quicker access to rows.
 **/
public class Index
{
    private final QualifiedName table;
    private final List<String> columns;
    private final int batchSize;
    private final ColumnsType columnsType;

    public Index(QualifiedName table, List<String> columns, ColumnsType columnsType, int batchSize)
    {
        this.table = requireNonNull(table, "table");
        this.columns = unmodifiableList(requireNonNull(columns, "columns"));
        this.columnsType = requireNonNull(columnsType, "columnsType");
        this.batchSize = batchSize;

        if (columnsType == ColumnsType.ANY)
        {
            throw new IllegalArgumentException("ANY columns is not supported yet.");
        }
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public ColumnsType getColumnsType()
    {
        return columnsType;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Index)
        {
            Index that = (Index) obj;
            return table.equals(that.table)
                && columns.equals(that.columns)
                && batchSize == that.batchSize
                && columnsType == that.columnsType;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table.toDotDelimited() + " " + columns.toString() + " (" + columnsType + ")";
    }

    /** Type of columns this index supports. */
    public enum ColumnsType
    {
        /**
         * A special type of index used by catalogs that can use all found columns for a table. Shortcut type to skip listing all the columns that the
         * table has. Used for example in JDBC-catalog.
         */
        WILDCARD,

        /**
         * Type that specifies that ALL columns must be used to be able to utilize this index
         */
        ALL,

        /**
         * Type that specifies that at least one column (ANY) must be used to be able to utilize this index
         */
        ANY
    }
}
