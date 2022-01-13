package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.OutputWriter;

/** Definition of a complex value that is written to a {@link OutputWriter} */
public interface ComplexValue
{
    /** Write this complex value to provided output writer */
    void write(OutputWriter outputWriter, ExecutionContext context);
}
