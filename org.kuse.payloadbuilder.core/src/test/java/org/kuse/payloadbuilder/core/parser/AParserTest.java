package org.kuse.payloadbuilder.core.parser;

import org.junit.Assert;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;

/** Base class for parser tests */
abstract class AParserTest extends Assert
{
    private final QueryParser p = new QueryParser();
    protected final QuerySession session = new QuerySession(new CatalogRegistry());
    protected final ExecutionContext context = new ExecutionContext(session);

    protected QueryStatement q(String query)
    {
        return p.parseQuery(session.getCatalogRegistry(), query);
    }

    protected Expression e(String expression)
    {
        return p.parseExpression(session.getCatalogRegistry(), expression);
    }
}
