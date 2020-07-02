package com.viskan.payloadbuilder.parser;

import static com.viskan.payloadbuilder.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;

/** Unit test of {@link ArithmeticUnaryExpression} */
public class ArithmeticUnaryExpressionTest extends AParserTest
{
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
}
