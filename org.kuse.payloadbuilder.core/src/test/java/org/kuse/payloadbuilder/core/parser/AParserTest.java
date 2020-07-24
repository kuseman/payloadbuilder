package org.kuse.payloadbuilder.core.parser;

import org.junit.Assert;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;

/** Base class for parser tests */
abstract class AParserTest extends Assert
{
    private final QueryParser p = new QueryParser();
    
    protected QueryStatement q(String query)
    {
        return p.parseQuery(query);
    }
    
    protected Expression e(String expression)
    {
        return p.parseExpression(expression);
    }
}
