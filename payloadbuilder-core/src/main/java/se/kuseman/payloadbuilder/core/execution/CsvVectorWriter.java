package se.kuseman.payloadbuilder.core.execution;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;
import se.kuseman.payloadbuilder.core.CsvOutputWriter;
import se.kuseman.payloadbuilder.core.CsvOutputWriter.CsvSettings;

/** {@link VectorWriter} that writes CSV output. */
class CsvVectorWriter extends AVectorWriter
{
    CsvVectorWriter(IExecutionContext context, OutputStream outputStream, List<Option> options)
    {
        super(context, new CsvOutputWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), CsvSettings.fromOptions(context, options)));
    }
}
