package se.kuseman.payloadbuilder.core.execution;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;
import se.kuseman.payloadbuilder.core.JsonOutputWriter.JsonSettings;

/** {@link VectorWriter} that writes JSON output. */
class JsonVectorWriter extends AVectorWriter
{
    JsonVectorWriter(ExecutionContext context, OutputStream outputStream, List<Option> options)
    {
        super(context, new JsonOutputWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), settings(context, options)));
    }

    /** Create settings from options. */
    private static JsonSettings settings(ExecutionContext context, List<Option> options)
    {
        JsonSettings settings = JsonSettings.fromOptions(context, options);
        // Force result set as arrays when writing vectors
        settings.setResultSetsAsArrays(true);
        return settings;
    }
}
