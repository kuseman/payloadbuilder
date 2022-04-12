package se.kuseman.payloadbuilder.core.parser;

import org.junit.Test;

/** Test of {@link LikeExpression} */
public class LikeExpressionTest extends AParserTest
{
    @Test
    public void test()
    {
        Expression e;

        e = e("null like 'test'");
        assertNull(e.eval(null));
        e = e("'test' like null");
        assertNull(e.eval(null));

        context.setVariable("var", "t__t");
        e = e("'test' like @var");
        assertTrue((boolean) e.eval(context));

        e = e("'test' like 'test'");
        assertTrue((boolean) e.eval(null));
        e = e("'test' like 'hello'");
        assertFalse((boolean) e.eval(null));

        e = e("'test' not like 'test'");
        assertFalse((boolean) e.eval(null));
        e = e("'test' not like 'hello'");
        assertTrue((boolean) e.eval(null));

        e = e("'test' like '%'");
        assertTrue((boolean) e.eval(null));
        e = e("'test' like '%t'");
        assertTrue((boolean) e.eval(null));
        e = e("'test' like '%et'");
        assertFalse((boolean) e.eval(null));
        e = e("'test' like 't%'");
        assertTrue((boolean) e.eval(null));
        e = e("'test' like 't%s%'");
        assertTrue((boolean) e.eval(null));
        e = e("'hello' like 't%s%'");
        assertFalse((boolean) e.eval(null));
        e = e("'hello' like 'h__lo'");
        assertTrue((boolean) e.eval(null));
        e = e("'helbo' like 'h__lo'");
        assertFalse((boolean) e.eval(null));
    }
}
