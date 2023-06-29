package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.core.catalog.ColumnReference;

/** Marker for expressions that has a {@link ColumnReference} */
public interface HasColumnReference
{
    /** Return column reference. */
    ColumnReference getColumnReference();
}
