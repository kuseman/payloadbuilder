package com.viskan.payloadbuilder.parser;

import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static com.viskan.payloadbuilder.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;

/** Unit test of {@link ComparisonExpression} */
public class ComparisonExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;
        
        e = e("1=1");
        assertTrue(e.isConstant());
        assertEquals(TRUE_LITERAL, e);
        e = e("1 > 1");
        assertEquals(FALSE_LITERAL, e);
        e = e("1=a");
        assertFalse(e.isConstant());
        assertEquals(e("1=a"), e);
        e = e("a=1");
        assertEquals(e("a=1"), e);
        assertFalse(e.isConstant());
        
        e = e("null=1");
        assertEquals(NULL_LITERAL, e);
        e = e("1=null");
        assertEquals(NULL_LITERAL, e);
        e = e("(1+2) > 10");
        assertEquals(FALSE_LITERAL, e);
        e = e("10 > (1+2)");
        assertEquals(TRUE_LITERAL, e);
        e = e("(1+2+a) > 10");
        assertEquals(e("(3+a) > 10"), e);
    }
}
