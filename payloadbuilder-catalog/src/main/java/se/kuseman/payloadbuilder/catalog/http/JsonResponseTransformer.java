package se.kuseman.payloadbuilder.catalog.http;

import org.apache.hc.core5.http.ContentType;

/** Transformer that transforms JSON responses */
public class JsonResponseTransformer extends ATableFunctionForwardResponseTransformer
{
    public JsonResponseTransformer()
    {
        super("openjson", ContentType.APPLICATION_JSON);
    }
}
