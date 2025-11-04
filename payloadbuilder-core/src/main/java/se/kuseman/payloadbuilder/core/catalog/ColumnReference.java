package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;

/**
 * Reference to a column on a table source.
 *
 * @param columnName Name of column
 * @param tableSourceReference Owning tableSourceReference for this columns
 */
public record ColumnReference(String columnName, TableSourceReference tableSourceReference, Column.MetaData metaData)
{
    /** Create a ColumnReference instance. */
    public ColumnReference
    {
        requireNonNull(columnName, "columnName");
        requireNonNull(tableSourceReference, "tableSourceReference");
        requireNonNull(metaData, "metaData");
    }
}
