package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.OutputWriter;

/** Definition of a projection */
public interface Projection
{
    Projection NO_OP_PROJECTION = (writer, context) ->
    {
    };

    /** Write value for provided writer and current execution context */
    void writeValue(OutputWriter writer, ExecutionContext context);
}
