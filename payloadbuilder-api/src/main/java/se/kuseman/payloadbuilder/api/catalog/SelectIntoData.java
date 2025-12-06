package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Various data provided when a {@link IDatasink} is requested for a Select into operator. */
public class SelectIntoData
{
    private final int nodeId;
    private final Schema inputSchema;
    private final List<Option> options;

    public SelectIntoData(int nodeId, Schema inputSchema, List<Option> options)
    {
        this.nodeId = nodeId;
        this.inputSchema = requireNonNull(inputSchema, "inputSchema");
        this.options = requireNonNull(options, "options");
    }

    /**
     * Return the node id for this sink. The operator ID that this data source belongs to. Can be used in {@link IStatementContext#getNodeData(Integer)} to store custom data.
     */
    public int getNodeId()
    {
        return nodeId;
    }

    /**
     * Return the schema of the input that is the source of the insert. If empty then this is an insert from an asterisk / schema-less query where the column is unknown at compile time, then the
     * actual columns are available from {@link TupleIterator} obtained in {@link IDatasink#execute} call.
     */
    public Schema getInputSchema()
    {
        return inputSchema;
    }

    /** Return defined options for this sink */
    public List<Option> getOptions()
    {
        return options;
    }
}
