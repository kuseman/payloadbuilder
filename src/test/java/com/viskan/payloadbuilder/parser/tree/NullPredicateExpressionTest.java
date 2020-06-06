package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.parser.QueryParser;

import static com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression.FALSE_LITERAL;
import static com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression.TRUE_LITERAL;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link NullPredicateExpression} */
public class NullPredicateExpressionTest extends Assert
{
    private final QueryParser p = new QueryParser();
    
    @Test
    public void test_fold()
    {   
        Expression e;
        
       e = e("null is null");
       assertTrue(e.isConstant());
       assertEquals(TRUE_LITERAL, e);
       
       e = e("null is not null");
       assertTrue(e.isConstant());
       assertEquals(FALSE_LITERAL, e);
       
       e = e("1 is null");
       assertTrue(e.isConstant());
       assertEquals(FALSE_LITERAL, e);
       
       e = e("1 is not null");
       assertTrue(e.isConstant());
       assertEquals(TRUE_LITERAL, e);
       
       e = e("1 + a is not null");
       assertFalse(e.isConstant());
       assertEquals(e("1 + a is not null"), e);
       
       e = e("1 + 1 + a is not null");
       assertFalse(e.isConstant());
       assertEquals(e("2 + a is not null"), e);
    }
    
    private Expression e(String expression)
    {
        return p.parseExpression(null, expression);
    }
}
