package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Marker for expressions that has a reference to a column in a schema. */
public interface HasColumnReference
{
    /** Return the column reference */
    ColumnReference getColumnReference();

    /** Reference to a column via a table source and a column type */
    record ColumnReference(TableSourceReference tableSourceReference, CoreColumn.Type columnType)
    {
        public ColumnReference
        {
            requireNonNull(tableSourceReference, "tableSourceReference");
            requireNonNull(columnType, "columnType");
        }
    }
}
