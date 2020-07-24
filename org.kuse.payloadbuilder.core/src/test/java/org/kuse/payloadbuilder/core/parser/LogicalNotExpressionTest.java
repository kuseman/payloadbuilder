package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import org.junit.Test;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.LogicalNotExpression;

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
