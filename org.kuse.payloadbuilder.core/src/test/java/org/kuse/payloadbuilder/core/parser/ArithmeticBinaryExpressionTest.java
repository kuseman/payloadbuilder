package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;
import org.kuse.payloadbuilder.core.parser.ArithmeticBinaryExpression;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Unit test of {@link ArithmeticBinaryExpression} */
public class ArithmeticBinaryExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;
        
        e = e("1+1");
        assertTrue(e.isConstant());
        assertEquals(e("2"), e);
        e = e("1+2+3+4+5+6");
        assertEquals(e("21"), e);
        e = e("1+2+3.1");
        assertEquals(e("6.1"), e);
        e = e("1+a");
        assertEquals(e("1+a"), e);
        assertFalse(e.isConstant());
        e = e("a+1");
        assertEquals(e("a+1"), e);
        assertFalse(e.isConstant());
        e = e("1+null");
        assertEquals(NULL_LITERAL, e);
        e = e("null+1");
        assertEquals(NULL_LITERAL, e);
        
        e = e("1+2+3+a");
        assertEquals(e("6+a"), e);
        
        e = e("1+2+3+a+null");
        assertEquals(NULL_LITERAL, e);
    }
    
    @Test(expected = ArithmeticException.class)
    public void test_fail()
    {
        e("1/0");
    }
}
