package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;

/** Base class for output writer. */
abstract class AVectorWriter implements VectorWriter
{
    private final IExecutionContext context;
    private final OutputWriter outputWriter;
    private boolean first = true;

    AVectorWriter(IExecutionContext context, OutputWriter outputWriter)
    {
        this.context = context;
        this.outputWriter = outputWriter;
    }

    @Override
    public void write(TupleVector vector)
    {
        if (first)
        {
            outputWriter.initResult(vector.getSchema()
                    .getColumns()
                    .stream()
                    .map(c -> c.getName())
                    .toArray(String[]::new));
            first = false;
        }
        // Write all vectors as root
        OutputWriterUtils.write(vector, outputWriter, context, true);
        outputWriter.flush();
    }

    @Override
    public void close()
    {
        outputWriter.close();
    }
}
