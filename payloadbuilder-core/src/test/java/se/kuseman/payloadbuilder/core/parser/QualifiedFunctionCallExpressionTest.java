package se.kuseman.payloadbuilder.core.parser;

import org.junit.Test;

/** Unit test of {@link QualifiedFunctionCallExpression} */
public class QualifiedFunctionCallExpressionTest extends AParserTest
{
    @Test
    public void test_fold()
    {
        Expression e;

        e = e("concat(1, 10)");
        assertEquals(e("concat(1,10)"), e);

        e = e("concat(1 + 1 + a, 10)");
        assertEquals(e("concat(2 + a, 10)"), e);

        e = e("a.filter(x -> 1+1+1+x > 10)");
        assertEquals(e("a.filter(x -> 3+x > 10)"), e);

        e = e("a.filter(x -> 1+1+1 > 10)");
        assertEquals(e("a.filter(x -> false)"), e);
    }
}
