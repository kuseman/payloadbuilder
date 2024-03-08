package se.kuseman.payloadbuilder.catalog.http;

import org.apache.hc.core5.http.ContentType;

/** Transformer that transforms CSV responses */
public class CsvResponseTransformer extends ATableFunctionForwardResponseTransformer
{
    public CsvResponseTransformer()
    {
        super("opencsv", ContentType.create("text/csv"));
    }
}
