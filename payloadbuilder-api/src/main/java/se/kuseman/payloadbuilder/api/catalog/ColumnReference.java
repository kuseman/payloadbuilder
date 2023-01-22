package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

/** Meta data about a column reference in an expression and schema */
public class ColumnReference
{
    private final TableSourceReference tableSource;
    private final String name;
    private final Type type;

    public ColumnReference(TableSourceReference tableSource, String name, Type type)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
        this.name = requireNonNull(name, "name");
        this.type = type;
    }

    public TableSourceReference getTableSource()
    {
        return tableSource;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isAsterisk()
    {
        return type == Type.ASTERISK;
    }

    /**
     * Rename this column reference. If this is an asterisk reference then {@link #type} will be change to {@link Type#NAMED_ASTERISK} otherwise type is unchanged.
     */
    public ColumnReference rename(String column)
    {
        return new ColumnReference(tableSource, column, type == Type.ASTERISK ? Type.NAMED_ASTERISK
                : type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableSource, name);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (obj instanceof ColumnReference)
        {
            ColumnReference that = (ColumnReference) obj;
            return tableSource.equals(that.tableSource)
                    && name.equals(that.name)
                    && type == that.type;
        }
        return false;
    }

    @Override
    public String toString()
    {
        // es#table1.column (*)
        return tableSource.toString() + "." + name;
    }

    /** Reference column type */
    public enum Type
    {
        /**
         * An asterisk column which is used in a schema less query for catalogs that doesn't have schemas. This acts as a place holder during planning where the actual columns will come runtime.
         */
        ASTERISK,

        /**
         * A named column that originates from an asterisk column. For example a projection on a table that has an asterisk column. It's still important to know that it's semi asterisk since we cannot
         * used it's ordinal i schema etc. since that is unknown during planning.
         */
        NAMED_ASTERISK,

        /** A regular column from a schema full table */
        REGULAR
    }
}
