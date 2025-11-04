package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

/**
 * Reference to a column on a table source and a column type.
 *
 * @param name Name of column
 * @param tableSourceReference Owning tableSourceReference for this columns
 * @param columnType The type of the column that was resolved
 */
public record ColumnReference(String name, TableSourceReference tableSourceReference, CoreColumn.Type columnType)
{
    /** Create a ColumnReference instance. */
    public ColumnReference
    {
        requireNonNull(name, "name");
        requireNonNull(tableSourceReference, "tableSourceReference");
        requireNonNull(columnType, "columnType");
    }
}
