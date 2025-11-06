package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

/**
 * Reference to a column on a table source.
 *
 * @param columnName Name of column
 * @param tableSourceReference Owning tableSourceReference for this columns
 */
public record ColumnReference(String columnName, TableSourceReference tableSourceReference)
{
    /** Create a ColumnReference instance. */
    public ColumnReference
    {
        requireNonNull(columnName, "columnName");
        requireNonNull(tableSourceReference, "tableSourceReference");
    }
}
