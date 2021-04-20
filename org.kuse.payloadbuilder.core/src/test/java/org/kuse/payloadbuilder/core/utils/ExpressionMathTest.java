package org.kuse.payloadbuilder.core.utils;

import static java.util.Arrays.asList;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link ExpressionMath} */
public class ExpressionMathTest extends Assert
{
    @Test
    public void test_inValue()
    {
        assertTrue(ExpressionMath.inValue(1, 1));
        assertTrue(ExpressionMath.inValue(1, "1"));
        assertFalse(ExpressionMath.inValue(1, 10));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2)));
        assertTrue(ExpressionMath.inValue(asList(1, 2), 1));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2).iterator()));
        assertFalse(ExpressionMath.inValue(1, asList(19, 2).iterator()));
        assertTrue(ExpressionMath.inValue(asList(1, 2).iterator(), 1));
        assertFalse(ExpressionMath.inValue(asList(19, 2).iterator(), 1));
    }

    @Test
    public void test_subtract()
    {
        assertNull(ExpressionMath.subtract(null, 1));
        assertNull(ExpressionMath.subtract(1, null));

        // int ->
        assertEquals(0, ExpressionMath.subtract(1, 1));
        assertEquals(0L, ExpressionMath.subtract(1, 1L));
        assertEquals(0F, ExpressionMath.subtract(1, 1F));
        assertEquals(0D, ExpressionMath.subtract(1, 1d));
        assertEquals(0D, ExpressionMath.subtract(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(0L, ExpressionMath.subtract(1L, 1));
        assertEquals(0L, ExpressionMath.subtract(1L, 1L));
        assertEquals(0F, ExpressionMath.subtract(1L, 1F));
        assertEquals(0D, ExpressionMath.subtract(1L, 1d));
        assertEquals(0D, ExpressionMath.subtract(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(0F, ExpressionMath.subtract(1F, 1));
        assertEquals(0F, ExpressionMath.subtract(1F, 1L));
        assertEquals(0F, ExpressionMath.subtract(1F, 1F));
        assertEquals(0D, ExpressionMath.subtract(1F, 1D));
        assertEquals(0D, ExpressionMath.subtract(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(0D, ExpressionMath.subtract(1D, 1));
        assertEquals(0D, ExpressionMath.subtract(1D, 1L));
        assertEquals(0D, ExpressionMath.subtract(1D, 1F));
        assertEquals(0D, ExpressionMath.subtract(1D, 1.00D));
        assertEquals(0D, ExpressionMath.subtract(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), 1));
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), 1L));
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), 1F));
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), 1.00D));
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(ArithmeticException.class, "Cannot subtract true and 1", () -> ExpressionMath.subtract(true, 1));
    }

    @Test
    public void test_multiply()
    {
        assertNull(ExpressionMath.multiply(null, 1));
        assertNull(ExpressionMath.multiply(1, null));

        // int ->
        assertEquals(1, ExpressionMath.multiply(1, 1));
        assertEquals(1L, ExpressionMath.multiply(1, 1L));
        assertEquals(1F, ExpressionMath.multiply(1, 1F));
        assertEquals(1D, ExpressionMath.multiply(1, 1d));
        assertEquals(1D, ExpressionMath.multiply(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(1L, ExpressionMath.multiply(1L, 1));
        assertEquals(1L, ExpressionMath.multiply(1L, 1L));
        assertEquals(1F, ExpressionMath.multiply(1L, 1F));
        assertEquals(1D, ExpressionMath.multiply(1L, 1d));
        assertEquals(1D, ExpressionMath.multiply(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(1F, ExpressionMath.multiply(1F, 1));
        assertEquals(1F, ExpressionMath.multiply(1F, 1L));
        assertEquals(1F, ExpressionMath.multiply(1F, 1F));
        assertEquals(1D, ExpressionMath.multiply(1F, 1D));
        assertEquals(1D, ExpressionMath.multiply(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(1D, ExpressionMath.multiply(1D, 1));
        assertEquals(1D, ExpressionMath.multiply(1D, 1L));
        assertEquals(1D, ExpressionMath.multiply(1D, 1F));
        assertEquals(1D, ExpressionMath.multiply(1D, 1.00D));
        assertEquals(1D, ExpressionMath.multiply(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), 1));
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), 1L));
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), 1F));
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), 1.00D));
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(ArithmeticException.class, "Cannot multiply true and 1", () -> ExpressionMath.multiply(true, 1));
    }

    @Test
    public void test_divide()
    {
        assertNull(ExpressionMath.divide(null, 1));
        assertNull(ExpressionMath.divide(1, null));

        // int ->
        assertEquals(1, ExpressionMath.divide(1, 1));
        assertEquals(1L, ExpressionMath.divide(1, 1L));
        assertEquals(1F, ExpressionMath.divide(1, 1F));
        assertEquals(1D, ExpressionMath.divide(1, 1d));
        assertEquals(1D, ExpressionMath.divide(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(1L, ExpressionMath.divide(1L, 1));
        assertEquals(1L, ExpressionMath.divide(1L, 1L));
        assertEquals(1F, ExpressionMath.divide(1L, 1F));
        assertEquals(1D, ExpressionMath.divide(1L, 1d));
        assertEquals(1D, ExpressionMath.divide(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(1F, ExpressionMath.divide(1F, 1));
        assertEquals(1F, ExpressionMath.divide(1F, 1L));
        assertEquals(1F, ExpressionMath.divide(1F, 1F));
        assertEquals(1D, ExpressionMath.divide(1F, 1D));
        assertEquals(1D, ExpressionMath.divide(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(1D, ExpressionMath.divide(1D, 1));
        assertEquals(1D, ExpressionMath.divide(1D, 1L));
        assertEquals(1D, ExpressionMath.divide(1D, 1F));
        assertEquals(1D, ExpressionMath.divide(1D, 1.00D));
        assertEquals(1D, ExpressionMath.divide(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), 1));
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), 1L));
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), 1F));
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), 1.00D));
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(ArithmeticException.class, "Cannot divide true and 1", () -> ExpressionMath.divide(true, 1));
    }

    @Test
    public void test_modulo()
    {
        assertNull(ExpressionMath.modulo(null, 1));
        assertNull(ExpressionMath.modulo(1, null));

        // int ->
        assertEquals(0, ExpressionMath.modulo(1, 1));
        assertEquals(0L, ExpressionMath.modulo(1, 1L));
        assertEquals(0F, ExpressionMath.modulo(1, 1F));
        assertEquals(0D, ExpressionMath.modulo(1, 1d));
        assertEquals(0D, ExpressionMath.modulo(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(0L, ExpressionMath.modulo(1L, 1));
        assertEquals(0L, ExpressionMath.modulo(1L, 1L));
        assertEquals(0F, ExpressionMath.modulo(1L, 1F));
        assertEquals(0D, ExpressionMath.modulo(1L, 1d));
        assertEquals(0D, ExpressionMath.modulo(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(0F, ExpressionMath.modulo(1F, 1));
        assertEquals(0F, ExpressionMath.modulo(1F, 1L));
        assertEquals(0F, ExpressionMath.modulo(1F, 1F));
        assertEquals(0D, ExpressionMath.modulo(1F, 1D));
        assertEquals(0D, ExpressionMath.modulo(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(0D, ExpressionMath.modulo(1D, 1));
        assertEquals(0D, ExpressionMath.modulo(1D, 1L));
        assertEquals(0D, ExpressionMath.modulo(1D, 1F));
        assertEquals(0D, ExpressionMath.modulo(1D, 1.00D));
        assertEquals(0D, ExpressionMath.modulo(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), 1));
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), 1L));
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), 1F));
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), 1.00D));
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(ArithmeticException.class, "Cannot modulo true and 1", () -> ExpressionMath.modulo(true, 1));
    }

    @Test
    public void test_add()
    {
        assertNull(ExpressionMath.add(null, 1));
        assertNull(ExpressionMath.add(1, null));

        // int ->
        assertEquals(2, ExpressionMath.add(1, 1));
        assertEquals(2L, ExpressionMath.add(1, 1L));
        assertEquals(2.1F, ExpressionMath.add(1, 1.1F));
        assertEquals(2.20D, ExpressionMath.add(1, 1.20d));
        assertEquals(2.30D, ExpressionMath.add(1, new BigDecimal("1.30")));

        // long ->
        assertEquals(2L, ExpressionMath.add(1L, 1));
        assertEquals(2L, ExpressionMath.add(1L, 1L));
        assertEquals(2.1F, ExpressionMath.add(1L, 1.1F));
        assertEquals(2.2D, ExpressionMath.add(1L, 1.20d));
        assertEquals(2.3D, ExpressionMath.add(1L, new BigDecimal("1.30")));

        // float ->
        assertEquals(2.1F, ExpressionMath.add(1.1F, 1));
        assertEquals(2.1F, ExpressionMath.add(1.1F, 1L));
        assertEquals(2.2F, ExpressionMath.add(1.1F, 1.1F));
        assertEquals(2.2D, ExpressionMath.add(1F, 1.20D));
        assertEquals(2.3D, ExpressionMath.add(1F, new BigDecimal("1.30")));

        // double ->
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1));
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1L));
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1F));
        assertEquals(2.2D, ExpressionMath.add(1D, 1.20D));
        assertEquals(2.3D, ExpressionMath.add(1D, new BigDecimal("1.30")));

        // big decimal ->
        assertEquals(2.1D, ExpressionMath.add(new BigDecimal("1.10"), 1));
        assertEquals(2.1D, ExpressionMath.add(new BigDecimal("1.10"), 1L));
        assertEquals(2.1D, ExpressionMath.add(new BigDecimal("1.10"), 1F));
        assertEquals(2.3D, ExpressionMath.add(new BigDecimal("1.10"), 1.20D));
        assertEquals(2.3D, ExpressionMath.add(new BigDecimal("1.00"), new BigDecimal("1.30")));

        assertEquals("hello1", ExpressionMath.add("hello", 1));
        assertEquals("1hello", ExpressionMath.add(1, "hello"));

        assertFail(ArithmeticException.class, "Cannot add true and 1", () -> ExpressionMath.add(true, 1));
    }

    @Test
    public void test_cmp_numbers()
    {
        assertEquals(0, ExpressionMath.cmp(1D, 1D));
        assertEquals(0, ExpressionMath.cmp(1D, 1F));
        assertEquals(0, ExpressionMath.cmp(1D, 1L));
        assertEquals(0, ExpressionMath.cmp(1D, 1));
        assertEquals(0, ExpressionMath.cmp(1D, new BigDecimal("1.00")));

        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1D));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1F));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1L));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertEquals(0, ExpressionMath.cmp(1F, 1D));
        assertEquals(0, ExpressionMath.cmp(1F, 1F));
        assertEquals(0, ExpressionMath.cmp(1F, 1L));
        assertEquals(0, ExpressionMath.cmp(1F, 1));
        assertEquals(0, ExpressionMath.cmp(1F, new BigDecimal("1.00")));

        assertEquals(0, ExpressionMath.cmp(1L, 1D));
        assertEquals(0, ExpressionMath.cmp(1L, 1F));
        assertEquals(0, ExpressionMath.cmp(1L, 1L));
        assertEquals(0, ExpressionMath.cmp(1L, 1));
        assertEquals(0, ExpressionMath.cmp(1L, new BigDecimal("1.00")));

        assertEquals(0, ExpressionMath.cmp(1, 1D));
        assertEquals(0, ExpressionMath.cmp(1, 1F));
        assertEquals(0, ExpressionMath.cmp(1, 1L));
        assertEquals(0, ExpressionMath.cmp(1, 1));
        assertEquals(0, ExpressionMath.cmp(1, new BigDecimal("1.00")));

        assertEquals(0, ExpressionMath.cmp("hello", "hello"));

        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(1D, ""));
        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(1F, ""));
        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(1L, ""));
        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(1, ""));
        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(new ExpressionMathTest(), new ExpressionMathTest()));
    }

    private void assertFail(Class<? extends Exception> expected, String messageContains, Runnable r)
    {
        try
        {
            r.run();
            fail();
        }
        catch (Exception e)
        {
            if (!e.getClass().isAssignableFrom(expected))
            {
                throw e;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + e.getMessage(), e.getMessage().contains(messageContains));
        }
    }
}
