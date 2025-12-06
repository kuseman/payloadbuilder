package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Various data provided when a {@link IDatasink} is requested for a Insert into operator. */
public class InsertIntoData extends SelectIntoData
{
    private final List<String> insertColumns;

    public InsertIntoData(int nodeId, Schema inputSchema, List<Option> options, List<String> insertColumns)
    {
        super(nodeId, inputSchema, options);
        this.insertColumns = requireNonNull(insertColumns, "insertColumns");
    }

    /** Returns the columns that will be inserted into table. */
    public List<String> getInsertColumns()
    {
        return insertColumns;
    }
}
