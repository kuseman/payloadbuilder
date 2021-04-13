package org.kuse.payloadbuilder.core.parser;

import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;

import org.junit.Test;

/** Unit test of {@link LogicalBinaryExpression} */
public class LogicalBinaryExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("true AND a");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("false AND a");
        assertEquals(FALSE_LITERAL, e);
        e = e("null AND a");
        assertEquals(e("null AND a"), e);
        e = e("null AND false");
        assertEquals(FALSE_LITERAL, e);

        e = e("true OR a");
        assertEquals(TRUE_LITERAL, e);
        e = e("false OR a");
        assertEquals(e("a"), e);
        e = e("null OR a");
        assertEquals(e("null oR a"), e);

        e = e("a AND true");
        assertFalse(e.isConstant());
        assertEquals(e("a"), e);
        e = e("a AND false");
        assertEquals(FALSE_LITERAL, e);
        e = e("a AND null");
        assertEquals(e("a AND null"), e);

        e = e("a OR true");
        assertEquals(TRUE_LITERAL, e);
        e = e("a OR false");
        assertEquals(e("a"), e);
        e = e("a OR null");
        assertEquals(e("a OR null"), e);

        e = e("a OR b");
        assertEquals(e("a OR b"), e);

        e = e("true OR false");
        assertTrue(e.isConstant());

        e = e("(a AND b AND c AND d) OR true");
        assertEquals(TRUE_LITERAL, e);

        e = e("(a AND b AND c AND d) OR false");
        assertEquals(e("(a AND b AND c AND d)"), e);

        e = e("(a AND true) OR (b and c)");
        assertFalse(e.isConstant());
        assertEquals(e("a OR (b and c)"), e);

        e = e("(pc.apply_to_articles = 1 or pc.includeSkuIds.contains(ap.sku_id)) and not pc.excludeSkuIds.contains(ap.sku_id)");
        assertFalse(e.isConstant());
        assertEquals(e("(pc.apply_to_articles = 1 or pc.includeSkuIds.contains(ap.sku_id)) and not pc.excludeSkuIds.contains(ap.sku_id)"), e);

        e = e("((a OR ((b))))");
        assertEquals(e("a OR b"), e);
    }
}
