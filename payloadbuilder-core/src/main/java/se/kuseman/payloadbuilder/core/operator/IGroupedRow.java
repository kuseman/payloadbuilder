package se.kuseman.payloadbuilder.core.operator;

import java.util.List;

import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Definition of a grouped row. A row that consists of other rows */
public interface IGroupedRow
{
    /** Return the contained rows */
    List<Tuple> getContainedRows();
}
