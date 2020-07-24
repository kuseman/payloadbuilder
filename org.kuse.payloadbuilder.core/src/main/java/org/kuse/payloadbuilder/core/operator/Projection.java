package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Definition of a projection */
public interface Projection
{
    /** Return value for provided row */
    void writeValue(OutputWriter writer, ExecutionContext context);
}
