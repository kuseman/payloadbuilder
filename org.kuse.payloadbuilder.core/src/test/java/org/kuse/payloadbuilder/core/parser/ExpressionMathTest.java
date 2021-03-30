package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link ExpressionMath} */
public class ExpressionMathTest extends Assert
{
    @Test
    public void test_inValue()
    {
        assertTrue(ExpressionMath.inValue(1, 1));
        assertFalse(ExpressionMath.inValue(1, 10));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2)));
        assertTrue(ExpressionMath.inValue(asList(1, 2), 1));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2).iterator()));
        assertFalse(ExpressionMath.inValue(1, asList(19, 2).iterator()));
        assertTrue(ExpressionMath.inValue(asList(1, 2).iterator(), 1));
        assertFalse(ExpressionMath.inValue(asList(19, 2).iterator(), 1));
    }

    @Test
    public void test_cmp_numbers()
    {
        assertEquals(0, ExpressionMath.cmp(1D, 1D));
        assertEquals(0, ExpressionMath.cmp(1D, 1F));
        assertEquals(0, ExpressionMath.cmp(1D, 1L));
        assertEquals(0, ExpressionMath.cmp(1D, 1));

        assertEquals(0, ExpressionMath.cmp(1F, 1D));
        assertEquals(0, ExpressionMath.cmp(1F, 1F));
        assertEquals(0, ExpressionMath.cmp(1F, 1L));
        assertEquals(0, ExpressionMath.cmp(1F, 1));

        assertEquals(0, ExpressionMath.cmp(1L, 1D));
        assertEquals(0, ExpressionMath.cmp(1L, 1F));
        assertEquals(0, ExpressionMath.cmp(1L, 1L));
        assertEquals(0, ExpressionMath.cmp(1L, 1));

        assertEquals(0, ExpressionMath.cmp(1, 1D));
        assertEquals(0, ExpressionMath.cmp(1, 1F));
        assertEquals(0, ExpressionMath.cmp(1, 1L));
        assertEquals(0, ExpressionMath.cmp(1, 1));
        assertEquals(0, ExpressionMath.cmp("hello", "hello"));

        assertCmpFail(1D, "");
        assertCmpFail(1F, "");
        assertCmpFail(1L, "");
        assertCmpFail(1, "");
        assertCmpFail(new ExpressionMathTest(), new ExpressionMathTest());
    }

    private void assertCmpFail(Object a, Object b)
    {
        try
        {
            ExpressionMath.cmp(a, b);
            fail();
        }
        catch (IllegalArgumentException e)
        {}
    }
}
