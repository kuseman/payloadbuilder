package org.kuse.payloadbuilder.core;

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.parser.QueryParser;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    private Payloadbuilder()
    {
    }

    /** Compile provided query qith with provided catgory registry */
    public static CompiledQuery compile(String query, CatalogRegistry registry)
    {
        return new CompiledQuery(PARSER.parseQuery(registry, query));
    }
}
