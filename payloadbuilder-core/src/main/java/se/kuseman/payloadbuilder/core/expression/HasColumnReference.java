package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Marker for expressions that has a reference to a column in a schema. */
public interface HasColumnReference
{
    /** Return the column reference */
    ColumnReference getColumnReference();

    /**
     * Reference to a column via a table source and a column type.
     *
     * @param tableSourceReference Owning tableSourceReference for this columns
     * @param columnType The type of the column that was resolved
     * @param tableTypeTableSource Optional table source pointing to the concrete table that this column reference points to. This can be a different than {{@link #tableSourceReference} if this was a
     * reference to an Expression scan for example.
     */
    record ColumnReference(TableSourceReference tableSourceReference, CoreColumn.Type columnType, TableSourceReference tableTypeTableSource)
    {
        public ColumnReference(TableSourceReference tableSourceReference, CoreColumn.Type columnType)
        {
            this(tableSourceReference, columnType, null);
        }

        /** Create a ColumnReference instance. */
        public ColumnReference
        {
            requireNonNull(tableSourceReference, "tableSourceReference");
            requireNonNull(columnType, "columnType");
            if (tableTypeTableSource != null
                    && tableTypeTableSource.getType() != TableSourceReference.Type.TABLE)
            {
                throw new IllegalArgumentException("tableTypeTableSource must be of type TABLE");
            }
        }
    }
}
