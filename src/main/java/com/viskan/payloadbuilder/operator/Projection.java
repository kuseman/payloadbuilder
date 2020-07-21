package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.parser.ExecutionContext;

/** Definition of a projection */
public interface Projection
{
    /** Return value for provided row */
    void writeValue(OutputWriter writer, ExecutionContext context);
}
