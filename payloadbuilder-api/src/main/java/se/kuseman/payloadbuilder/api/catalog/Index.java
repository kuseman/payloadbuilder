package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Set;

import se.kuseman.payloadbuilder.api.QualifiedName;

/**
 * A table index. Defines columns that can be utilized in batching operators for quicker access to rows.
 **/
public class Index
{
    private final QualifiedName table;
    private final List<String> columns;
    private final ColumnsType columnsType;
    private final Set<IndexType> indexTypes;

    public Index(QualifiedName table, List<String> columns, ColumnsType columnsType)
    {
        this(table, columns, columnsType, Set.of(IndexType.SEEK_EQ));
    }

    public Index(QualifiedName table, List<String> columns, ColumnsType columnsType, Set<IndexType> indexTypes)
    {
        this.table = requireNonNull(table, "table");
        this.columns = unmodifiableList(requireNonNull(columns, "columns"));
        this.columnsType = requireNonNull(columnsType, "columnsType");
        this.indexTypes = unmodifiableSet(requireNonNull(indexTypes, "indexTypes"));
        if (indexTypes.isEmpty())
        {
            throw new IllegalArgumentException("Index types cannot be empty");
        }

        if (columns.isEmpty()
                && columnsType != ColumnsType.WILDCARD)
        {
            throw new IllegalArgumentException("Non wildcard indices must have atleast one column");
        }
        else if (!columns.isEmpty()
                && columnsType == ColumnsType.WILDCARD)
        {
            throw new IllegalArgumentException("Wildcard indices cannot have columns");
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

    public boolean supports(IndexType type)
    {
        return indexTypes.contains(type);
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof Index that)
        {
            return table.equals(that.table)
                    && columns.equals(that.columns)
                    && columnsType == that.columnsType;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table + " " + columns.toString() + " (" + columnsType + ")";
    }

    /** Index type. */
    public enum IndexType
    {
        /** Specifies that seek with EQ is supported. */
        SEEK_EQ
    }

    /** Type of columns this index supports. */
    public enum ColumnsType
    {
        /**
         * A special type of index used by catalogs that can use all found columns for a table. Shortcut type to skip listing all the columns that the table has. Used for example in JDBC-catalog.
         */
        WILDCARD,

        /**
         * Type that specifies that ALL columns must be used to be able to utilize this index
         */
        ALL,

        /**
         * Type that specifies that at least one column (ANY) must be used to be able to utilize this index
         */
        ANY,

        /**
         * Type that specifies that at least one column (ANY) must be used to be able to utilize this index but columns must be used in order from left to right.
         */
        ANY_IN_ORDER
    }
}
