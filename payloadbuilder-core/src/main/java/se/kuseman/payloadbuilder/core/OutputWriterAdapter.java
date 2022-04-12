package se.kuseman.payloadbuilder.core;

import se.kuseman.payloadbuilder.api.OutputWriter;

/** Adapter for {@link OutputWriter} */
public class OutputWriterAdapter implements OutputWriter
{
    public static final OutputWriter NO_OP_WRITER = new OutputWriterAdapter();

    @Override
    public void writeFieldName(String name)
    {
    }

    @Override
    public void writeValue(Object value)
    {
    }

    @Override
    public void writeDouble(double value)
    {
    }

    @Override
    public void writeFloat(float value)
    {
    }

    @Override
    public void writeLong(long value)
    {
    }

    @Override
    public void writeInt(int value)
    {
    }

    @Override
    public void startObject()
    {
    }

    @Override
    public void endObject()
    {
    }

    @Override
    public void startArray()
    {
    }

    @Override
    public void endArray()
    {
    }
}
