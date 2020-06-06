package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.parser.QueryParser;

import static com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression.FALSE_LITERAL;
import static com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression.TRUE_LITERAL;
import static com.viskan.payloadbuilder.parser.tree.LiteralNullExpression.NULL_LITERAL;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link InExpression} */
public class InExpressionTest extends Assert
{
    private final QueryParser p = new QueryParser();
    
    @Test
    public void test_fold()
    {
        Expression e;
        
        e = e("1 in (null, 1,2,3)");
        assertTrue(e.isConstant());
        assertEquals(TRUE_LITERAL, e);
        
        e = e("7 in (null, 1,2,3)");
        assertTrue(e.isConstant());
        assertEquals(FALSE_LITERAL, e);
        
        e = e("a in (null, 1,2,3)");
        assertFalse(e.isConstant());
        assertEquals(e("a in (null, 1,2,3)"), e);

        e = e("a in (null, 1,c,2)");
        assertFalse(e.isConstant());
        assertEquals(e("a in (null, 1,c,2)"), e);
        
        e = e("a in (null, 1 +2,2,3)");
        assertEquals(e("a in (null, 3,2,3)"), e);

        e = e("null in (null, 1 +2,2,3)");
        assertEquals(NULL_LITERAL, e);
        
        e = e("(1 + 1 + a) in (null, 1 +2,2,c)");
        assertFalse(e.isConstant());
        assertEquals(e("(2 + a) in (null, 3,2,c)"), e);
        
    }
    
    private Expression e(String expression)
    {
        return p.parseExpression(null, expression);
    }
}
