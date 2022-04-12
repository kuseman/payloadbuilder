package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import org.junit.Test;

/** Unit test of {@link CaseExpression} */
public class CaseExpressionTest extends AParserTest
{
    @Test
    public void test_evaluate()
    {
        Expression e;

        e = e("case when true then 10 end");

        assertEquals(new CaseExpression(asList(new CaseExpression.WhenClause(e("true"), e("10"))), null), e);
        assertNotEquals(new CaseExpression(asList(new CaseExpression.WhenClause(e("false"), e("10"))), null), e);

        assertTrue(e.isConstant());
        assertEquals(10, e.eval(null));

        e = e("case when false then 10 end");
        assertTrue(e.isConstant());
        assertNull(e.eval(null));

        e = e("case when false then 10 else 20 end");

        assertEquals(new CaseExpression(asList(new CaseExpression.WhenClause(e("false"), e("10"))), e("20")), e);
        assertNotEquals(new CaseExpression(asList(new CaseExpression.WhenClause(e("false"), e("10"))), e("30")), e);
        assertNotEquals(new CaseExpression(asList(new CaseExpression.WhenClause(e("false"), e("20"))), e("20")), e);

        assertTrue(e.isConstant());
        assertEquals(20, e.eval(null));

        context.setVariable("var", false);
        e = e("case when @var then 10 else 20 end");
        assertFalse(e.isConstant());
        assertEquals(20, e.eval(context));

        context.setVariable("var", 20);
        e = e("case when true then @var else 20 end");
        assertFalse(e.isConstant());
        assertEquals(20, e.eval(context));

        context.setVariable("var", 20);
        e = e("case when false then 10 else @var end");
        assertFalse(e.isConstant());
        assertEquals(20, e.eval(context));
    }
}
