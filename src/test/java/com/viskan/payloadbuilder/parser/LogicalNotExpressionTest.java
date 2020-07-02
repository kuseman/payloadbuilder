package com.viskan.payloadbuilder.parser;

import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static com.viskan.payloadbuilder.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;

/** Unit test of {@link LogicalNotExpression} */
public class LogicalNotExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {   
        Expression e;
        
        e = e("not true");
        assertTrue(e.isConstant());
        assertEquals(FALSE_LITERAL, e);
        e = e("not false");
        assertEquals(TRUE_LITERAL, e);
        e = e("not a");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
        e = e("not null");
        assertEquals(NULL_LITERAL, e);
        
        e = e("not (true AND a)");
        assertFalse(e.isConstant());
        assertEquals(e("not a"), e);
    }
}
