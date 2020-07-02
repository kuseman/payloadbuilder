package com.viskan.payloadbuilder.parser;

import org.junit.Assert;

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
