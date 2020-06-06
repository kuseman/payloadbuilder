package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.parser.QueryParser;

import static com.viskan.payloadbuilder.parser.tree.LiteralNullExpression.NULL_LITERAL;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link ArithmeticUnaryExpression} */
public class ArithmeticUnaryExpressionTest extends Assert
{
    private final QueryParser p = new QueryParser();
    
    @Test
    public void test_fold()
    {
        Expression e;
        
        e = e("-(1+2)");
        assertTrue(e.isConstant());
        assertEquals(e("-3"), e);
 
        e = e("-null");
        assertEquals(NULL_LITERAL, e);
        
        e = e("-a");
        assertFalse(e.isConstant());
        assertEquals(e("-a"), e);
        
        e = e("-(1+2+a)");
        assertEquals(e("-(3+a)"), e);
    }

    private Expression e(String expression)
    {
        return p.parseExpression(null, expression);
    }
}
