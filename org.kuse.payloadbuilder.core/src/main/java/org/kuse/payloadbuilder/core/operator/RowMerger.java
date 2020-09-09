package org.kuse.payloadbuilder.core.operator;

/** Definition of a row merger that merges to joined rows. */
public interface RowMerger
{
    /**
     * Merge outer and inner row.
     *
     * @param outer Outer row that is joined
     * @param inner Inner row that is joined
     * @param populating True if the join is populating. ie inner row is added to outer without creating a new tuple.
     **/
    Row merge(Row outer, Row inner, boolean populating);
}
