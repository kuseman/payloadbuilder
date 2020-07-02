package com.viskan.payloadbuilder.parser;

import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static com.viskan.payloadbuilder.parser.LiteralBooleanExpression.TRUE_LITERAL;

import org.junit.Test;

/** Unit test of {@link NullPredicateExpression} */
public class NullPredicateExpressionTest extends AParserTest
{
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
}
