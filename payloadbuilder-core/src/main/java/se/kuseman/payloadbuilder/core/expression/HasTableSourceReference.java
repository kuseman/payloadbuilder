package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Marker for expressions that has a {@link TableSourceReference} */
public interface HasTableSourceReference
{
    /** Return table source reference. */
    TableSourceReference getTableSourceReference();
}
