package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;

import org.junit.Test;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.NullPredicateExpression;

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
