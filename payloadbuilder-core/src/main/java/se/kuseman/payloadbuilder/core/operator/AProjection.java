package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.OutputWriter;

/** Base projection */
abstract class AProjection implements Projection
{
    /**
     * Writes provided value to writer. Checks if the value is of a complex type and streams accordingly
     */
    protected void writeValue(OutputWriter writer, ExecutionContext context, Object value)
    {
        if (value instanceof ComplexValue)
        {
            ComplexValue complexValue = (ComplexValue) value;
            complexValue.write(writer, context);
        }
        else
        {
            writer.writeValue(value);
        }
    }
}
