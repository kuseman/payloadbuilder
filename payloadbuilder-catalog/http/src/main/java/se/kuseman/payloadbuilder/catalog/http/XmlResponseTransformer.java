package se.kuseman.payloadbuilder.catalog.http;

import org.apache.hc.core5.http.ContentType;

/** Transformer that transforms JSON responses */
public class XmlResponseTransformer extends ATableFunctionForwardResponseTransformer
{
    public XmlResponseTransformer()
    {
        super("openxml", ContentType.TEXT_XML);
    }
}
