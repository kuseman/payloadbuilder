package org.kuse.payloadbuilder.core.parser;

import org.junit.Assert;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;

/** Base class for parser tests */
abstract class AParserTest extends Assert
{
    private final QueryParser p = new QueryParser();
    protected final ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));

    protected QueryStatement q(String query)
    {
        return p.parseQuery(query);
    }

    protected Expression e(String expression)
    {
        return p.parseExpression(expression);
    }
}
