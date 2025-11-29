package se.kuseman.payloadbuilder.core.execution;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;
import se.kuseman.payloadbuilder.core.PlainTextOutputWriter;

/** {@link VectorWriter} that writes PLAIN/Text output. */
class TextVectorWriter extends AVectorWriter
{
    TextVectorWriter(ExecutionContext context, OutputStream outputStream, List<Option> options)
    {
        super(context, new PlainTextOutputWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)));
    }
}
