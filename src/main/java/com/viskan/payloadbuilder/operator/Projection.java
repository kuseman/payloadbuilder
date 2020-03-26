package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

/** Definition of a projection */
public interface Projection
{
    /** Return value for provided row */
    void writeValue(OutputWriter writer, OperatorContext context, Row row);
}
