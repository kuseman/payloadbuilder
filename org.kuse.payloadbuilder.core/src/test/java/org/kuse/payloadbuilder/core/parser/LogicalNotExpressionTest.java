package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

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

        e = e("not not a");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);

        e = e("not a = 1");
        assertEquals(e("a != 1"), e);
        e = e("not a != 1");
        assertEquals(e("a = 1"), e);
        e = e("not a > 0");
        assertEquals(e("a <= 0"), e);
        e = e("not a >= 0");
        assertEquals(e("a < 0"), e);
        e = e("not a < 0");
        assertEquals(e("a >= 0"), e);
        e = e("not a <= 0");
        assertEquals(e("a > 0"), e);

        e = e("not (a and b)");
        assertEquals(e("not a or not b"), e);

        e = e("not (a or b)");
        assertEquals(e("not a and not b"), e);

        e = e("not (a in(1,2,3))");
        assertEquals(e("a not in (1,2,3)"), e);

        e = e("not (a like '%text%')");
        assertEquals(e("a not like '%text%'"), e);
    }
}
