package se.kuseman.payloadbuilder.core.execution;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Unit test of {@link ExpressionMath} */
public class ExpressionMathTest extends Assert
{
    @Test
    public void test_negate()
    {
        assertEquals(-1, ExpressionMath.negate(1));
        assertEquals(-1L, ExpressionMath.negate(1L));
        assertEquals(-1.0F, ExpressionMath.negate(1.0F));
        assertEquals(-1.0D, ExpressionMath.negate(1D));
        assertEquals(Decimal.from(new BigDecimal("-1.0")), ExpressionMath.negate(new BigDecimal("1.0")));

        assertFail(IllegalArgumentException.class, "Cannot negate 'true'", () -> ExpressionMath.negate(true));
    }

    @Test
    public void test_abs()
    {
        assertEquals(1, ExpressionMath.abs(-1));
        assertEquals(1L, ExpressionMath.abs(-1L));
        assertEquals(1.0F, ExpressionMath.abs(-1.0F));
        assertEquals(1.0D, ExpressionMath.abs(-1D));
        assertEquals(Decimal.from(new BigDecimal("1.0")), ExpressionMath.abs(new BigDecimal("-1.0")));

        assertFail(IllegalArgumentException.class, "Cannot return absolute value of: 'true'", () -> ExpressionMath.abs(true));
    }

    @Test
    public void test_ceiling()
    {
        assertEquals(1, ExpressionMath.ceiling(1));
        assertEquals(1L, ExpressionMath.ceiling(1L));
        assertEquals(2.0F, ExpressionMath.ceiling(1.5F));
        assertEquals(3.0D, ExpressionMath.ceiling(2.5D));
        assertEquals(Decimal.from(new BigDecimal("4")), ExpressionMath.ceiling(new BigDecimal("3.5")));

        assertFail(IllegalArgumentException.class, "Cannot return ceiling value of: 'true'", () -> ExpressionMath.ceiling(true));
    }

    @Test
    public void test_floor()
    {
        assertEquals(1, ExpressionMath.floor(1));
        assertEquals(1L, ExpressionMath.floor(1L));
        assertEquals(1.0F, ExpressionMath.floor(1.5F));
        assertEquals(2.0D, ExpressionMath.floor(2.5D));
        assertEquals(Decimal.from(new BigDecimal("3")), ExpressionMath.floor(new BigDecimal("3.5")));

        assertFail(IllegalArgumentException.class, "Cannot return floor value of: 'true'", () -> ExpressionMath.floor(true));
    }

    @Test
    public void test_subtract()
    {
        // int ->
        assertEquals(0, ExpressionMath.subtract(1, 1));
        assertEquals(0L, ExpressionMath.subtract(1, 1L));
        assertEquals(0F, ExpressionMath.subtract(1, 1F));
        assertEquals(0D, ExpressionMath.subtract(1, 1d));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.subtract(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(0L, ExpressionMath.subtract(1L, 1));
        assertEquals(0L, ExpressionMath.subtract(1L, 1L));
        assertEquals(0F, ExpressionMath.subtract(1L, 1F));
        assertEquals(0D, ExpressionMath.subtract(1L, 1d));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.subtract(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(0F, ExpressionMath.subtract(1F, 1));
        assertEquals(0F, ExpressionMath.subtract(1F, 1L));
        assertEquals(0F, ExpressionMath.subtract(1F, 1F));
        assertEquals(0D, ExpressionMath.subtract(1F, 1D));
        assertEquals(0F, ExpressionMath.subtract(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(0D, ExpressionMath.subtract(1D, 1));
        assertEquals(0D, ExpressionMath.subtract(1D, 1L));
        assertEquals(0D, ExpressionMath.subtract(1D, 1F));
        assertEquals(0D, ExpressionMath.subtract(1D, 1.00D));
        assertEquals(0D, ExpressionMath.subtract(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(Decimal.from("0.000000"), ExpressionMath.subtract(new BigDecimal("1.00"), 1));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.subtract(new BigDecimal("1.00"), 1L));
        assertEquals(0F, ExpressionMath.subtract(new BigDecimal("1.00"), 1F));
        assertEquals(0D, ExpressionMath.subtract(new BigDecimal("1.00"), 1.00D));
        assertEquals(Decimal.from("0.00"), ExpressionMath.subtract(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types TestComparable and TestNonComparable",
                () -> ExpressionMath.subtract(new TestComparable(), new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Integer and TestNonComparable", () -> ExpressionMath.subtract(10, new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Boolean and String", () -> ExpressionMath.subtract(true, "hello"));
    }

    @Test
    public void test_multiply()
    {
        // int ->
        assertEquals(1, ExpressionMath.multiply(1, 1));
        assertEquals(1L, ExpressionMath.multiply(1, 1L));
        assertEquals(1F, ExpressionMath.multiply(1, 1F));
        assertEquals(1D, ExpressionMath.multiply(1, 1d));
        assertEquals(Decimal.from("1.00000000"), ExpressionMath.multiply(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(1L, ExpressionMath.multiply(1L, 1));
        assertEquals(1L, ExpressionMath.multiply(1L, 1L));
        assertEquals(1F, ExpressionMath.multiply(1L, 1F));
        assertEquals(1D, ExpressionMath.multiply(1L, 1d));
        assertEquals(Decimal.from("1.00000000"), ExpressionMath.multiply(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(1F, ExpressionMath.multiply(1F, 1));
        assertEquals(1F, ExpressionMath.multiply(1F, 1L));
        assertEquals(1F, ExpressionMath.multiply(1F, 1F));
        assertEquals(1D, ExpressionMath.multiply(1F, 1D));
        assertEquals(1F, ExpressionMath.multiply(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(1D, ExpressionMath.multiply(1D, 1));
        assertEquals(1D, ExpressionMath.multiply(1D, 1L));
        assertEquals(1D, ExpressionMath.multiply(1D, 1F));
        assertEquals(1D, ExpressionMath.multiply(1D, 1.00D));
        assertEquals(1D, ExpressionMath.multiply(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(Decimal.from("1.00000000"), ExpressionMath.multiply(new BigDecimal("1.00"), 1));
        assertEquals(Decimal.from("1.00000000"), ExpressionMath.multiply(new BigDecimal("1.00"), 1L));
        assertEquals(1F, ExpressionMath.multiply(new BigDecimal("1.00"), 1F));
        assertEquals(1D, ExpressionMath.multiply(new BigDecimal("1.00"), 1.00D));
        assertEquals(Decimal.from("1.0000"), ExpressionMath.multiply(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types TestComparable and TestNonComparable",
                () -> ExpressionMath.multiply(new TestComparable(), new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Integer and TestNonComparable", () -> ExpressionMath.multiply(10, new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Boolean and String", () -> ExpressionMath.multiply(true, "hello"));
    }

    @Test
    public void test_divide()
    {
        // int ->
        assertEquals(1, ExpressionMath.divide(1, 1));
        assertEquals(1L, ExpressionMath.divide(1, 1L));
        assertEquals(1F, ExpressionMath.divide(1, 1F));
        assertEquals(1D, ExpressionMath.divide(1, 1d));
        assertEquals(Decimal.from("1.000000"), ExpressionMath.divide(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(1L, ExpressionMath.divide(1L, 1));
        assertEquals(1L, ExpressionMath.divide(1L, 1L));
        assertEquals(1F, ExpressionMath.divide(1L, 1F));
        assertEquals(1D, ExpressionMath.divide(1L, 1d));
        assertEquals(Decimal.from("1.000000"), ExpressionMath.divide(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(1F, ExpressionMath.divide(1F, 1));
        assertEquals(1F, ExpressionMath.divide(1F, 1L));
        assertEquals(1F, ExpressionMath.divide(1F, 1F));
        assertEquals(1D, ExpressionMath.divide(1F, 1D));
        assertEquals(1F, ExpressionMath.divide(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(1D, ExpressionMath.divide(1D, 1));
        assertEquals(1D, ExpressionMath.divide(1D, 1L));
        assertEquals(1D, ExpressionMath.divide(1D, 1F));
        assertEquals(1D, ExpressionMath.divide(1D, 1.00D));
        assertEquals(1D, ExpressionMath.divide(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(Decimal.from("1.00"), ExpressionMath.divide(new BigDecimal("1.00"), 1));
        assertEquals(Decimal.from("1.00"), ExpressionMath.divide(new BigDecimal("1.00"), 1L));
        assertEquals(1F, ExpressionMath.divide(new BigDecimal("1.00"), 1F));
        assertEquals(1D, ExpressionMath.divide(new BigDecimal("1.00"), 1.00D));
        assertEquals(Decimal.from("1.00"), ExpressionMath.divide(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types TestComparable and TestNonComparable",
                () -> ExpressionMath.divide(new TestComparable(), new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Integer and TestNonComparable", () -> ExpressionMath.divide(10, new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Boolean and String", () -> ExpressionMath.divide(true, "hello"));
    }

    @Test
    public void test_modulo()
    {
        // int ->
        assertEquals(0, ExpressionMath.modulo(1, 1));
        assertEquals(0L, ExpressionMath.modulo(1, 1L));
        assertEquals(0F, ExpressionMath.modulo(1, 1F));
        assertEquals(0D, ExpressionMath.modulo(1, 1d));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.modulo(1, new BigDecimal("1.00")));

        // long ->
        assertEquals(0L, ExpressionMath.modulo(1L, 1));
        assertEquals(0L, ExpressionMath.modulo(1L, 1L));
        assertEquals(0F, ExpressionMath.modulo(1L, 1F));
        assertEquals(0D, ExpressionMath.modulo(1L, 1d));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.modulo(1L, new BigDecimal("1.00")));

        // float ->
        assertEquals(0F, ExpressionMath.modulo(1F, 1));
        assertEquals(0F, ExpressionMath.modulo(1F, 1L));
        assertEquals(0F, ExpressionMath.modulo(1F, 1F));
        assertEquals(0D, ExpressionMath.modulo(1F, 1D));
        assertEquals(0F, ExpressionMath.modulo(1F, new BigDecimal("1.00")));

        // double ->
        assertEquals(0D, ExpressionMath.modulo(1D, 1));
        assertEquals(0D, ExpressionMath.modulo(1D, 1L));
        assertEquals(0D, ExpressionMath.modulo(1D, 1F));
        assertEquals(0D, ExpressionMath.modulo(1D, 1.00D));
        assertEquals(0D, ExpressionMath.modulo(1D, new BigDecimal("1.00")));

        // big decimal ->
        assertEquals(Decimal.from("0.000000"), ExpressionMath.modulo(new BigDecimal("1.00"), 1));
        assertEquals(Decimal.from("0.000000"), ExpressionMath.modulo(new BigDecimal("1.00"), 1L));
        assertEquals(0F, ExpressionMath.modulo(new BigDecimal("1.00"), 1F));
        assertEquals(0D, ExpressionMath.modulo(new BigDecimal("1.00"), 1.00D));
        assertEquals(Decimal.from("0.00"), ExpressionMath.modulo(new BigDecimal("1.00"), new BigDecimal("1.00")));

        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types TestComparable and TestNonComparable",
                () -> ExpressionMath.modulo(new TestComparable(), new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Integer and TestNonComparable", () -> ExpressionMath.modulo(10, new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Boolean and String", () -> ExpressionMath.modulo(true, "hello"));
    }

    @Test
    public void test_add()
    {
        // int ->
        assertEquals(2, ExpressionMath.add(1, 1));
        assertEquals(2L, ExpressionMath.add(1, 1L));
        assertEquals(2.1F, ExpressionMath.add(1, 1.1F));
        assertEquals(2.20D, ExpressionMath.add(1, 1.20d));
        assertEquals(Decimal.from("2.300000"), ExpressionMath.add(1, new BigDecimal("1.30")));

        // long ->
        assertEquals(2L, ExpressionMath.add(1L, 1));
        assertEquals(2L, ExpressionMath.add(1L, 1L));
        assertEquals(2.1F, ExpressionMath.add(1L, 1.1F));
        assertEquals(2.2D, ExpressionMath.add(1L, 1.20d));
        assertEquals(Decimal.from("2.300000"), ExpressionMath.add(1L, new BigDecimal("1.30")));

        // float ->
        assertEquals(2.1F, ExpressionMath.add(1.1F, 1));
        assertEquals(2.1F, ExpressionMath.add(1.1F, 1L));
        assertEquals(2.2F, ExpressionMath.add(1.1F, 1.1F));
        assertEquals(2.2D, ExpressionMath.add(1F, 1.20D));
        assertEquals(2.3F, ExpressionMath.add(1F, new BigDecimal("1.30")));

        // double ->
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1));
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1L));
        assertEquals(2.1D, ExpressionMath.add(1.1D, 1F));
        assertEquals(2.2D, ExpressionMath.add(1D, 1.20D));
        assertEquals(2.3D, ExpressionMath.add(1D, new BigDecimal("1.30")));

        // big decimal ->
        assertEquals(Decimal.from("2.100000"), ExpressionMath.add(new BigDecimal("1.10"), 1));
        assertEquals(Decimal.from("2.100000"), ExpressionMath.add(new BigDecimal("1.10"), 1L));
        assertEquals(2.1F, ExpressionMath.add(new BigDecimal("1.10"), 1F));
        assertEquals(2.3D, ExpressionMath.add(new BigDecimal("1.10"), 1.20D));
        assertEquals(Decimal.from("2.30"), ExpressionMath.add(new BigDecimal("1.00"), new BigDecimal("1.30")));

        assertEquals(UTF8String.from("hello world"), ExpressionMath.add("hello", " world"));
        assertEquals(21, ExpressionMath.add(1, "20"));

        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types TestComparable and TestNonComparable", () -> ExpressionMath.add(new TestComparable(), new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Integer and TestNonComparable", () -> ExpressionMath.add(10, new TestNonComparable()));
        assertFail(IllegalArgumentException.class, "Cannot perform arithmetics on types Boolean and String", () -> ExpressionMath.add(true, "hello"));
    }

    @Test
    public void test_cmp_non_plb_types()
    {
        assertEquals(-1, ExpressionMath.cmp(new TestComparable(), 10));
        assertEquals(1, ExpressionMath.cmp(new TestNonComparable(), new TestComparable()));
        assertFail(ClassCastException.class, "", () -> ExpressionMath.cmp(10, new TestComparable()));
        assertEquals(0, ExpressionMath.cmp(new TestComparable(), new TestComparable()));
        assertEquals(1, ExpressionMath.cmp(true, false));
    }

    @Test
    public void test_cmp_dates()
    {
        assertEquals(0, ExpressionMath.cmp(EpochDateTime.from(100000000), EpochDateTime.from(100000000)));
        assertEquals(0, ExpressionMath.cmp(EpochDateTimeOffset.from(100000000), EpochDateTimeOffset.from(100000000)));
    }

    @Test
    public void test_cmp_numbers()
    {
        assertEquals(0, ExpressionMath.cmp(1D, 1D));
        assertEquals(0, ExpressionMath.cmp(1D, 1F));
        assertEquals(0, ExpressionMath.cmp(1D, 1L));
        assertEquals(0, ExpressionMath.cmp(1D, 1));
        assertEquals(0, ExpressionMath.cmp(1D, new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(1D, Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1D));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1F));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1L));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), 1));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(new BigDecimal("1.00"), Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp(1F, 1D));
        assertEquals(0, ExpressionMath.cmp(1F, 1F));
        assertEquals(0, ExpressionMath.cmp(1F, 1L));
        assertEquals(0, ExpressionMath.cmp(1F, 1));
        assertEquals(0, ExpressionMath.cmp(1F, new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(1F, Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp(1L, 1D));
        assertEquals(0, ExpressionMath.cmp(1L, 1F));
        assertEquals(0, ExpressionMath.cmp(1L, 1L));
        assertEquals(0, ExpressionMath.cmp(1L, 1));
        assertEquals(0, ExpressionMath.cmp(1L, new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(1L, Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp(1, 1D));
        assertEquals(0, ExpressionMath.cmp(1, 1F));
        assertEquals(0, ExpressionMath.cmp(1, 1L));
        assertEquals(0, ExpressionMath.cmp(1, 1));
        assertEquals(0, ExpressionMath.cmp(1, new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(1, Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), 1D));
        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), 1F));
        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), 1L));
        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), 1));
        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), new BigDecimal("1.00")));
        assertEquals(0, ExpressionMath.cmp(Decimal.from(1), Decimal.from(new BigDecimal("1.00"))));

        assertEquals(0, ExpressionMath.cmp("hello", "hello"));

        assertFail(IllegalArgumentException.class, "Cannot cast ", () -> ExpressionMath.cmp(1D, ""));
        assertFail(IllegalArgumentException.class, "Cannot cast ", () -> ExpressionMath.cmp(1F, ""));
        assertFail(IllegalArgumentException.class, "Cannot cast ", () -> ExpressionMath.cmp(1L, ""));
        assertFail(IllegalArgumentException.class, "Cannot cast ", () -> ExpressionMath.cmp(1, ""));
        assertFail(IllegalArgumentException.class, "Cannot compare ", () -> ExpressionMath.cmp(new ExpressionMathTest(), new ExpressionMathTest()));
    }

    static class TestComparable implements Comparable<Object>
    {
        @Override
        public int compareTo(Object o)
        {
            if (o instanceof TestComparable)
            {
                return 0;
            }
            return -1;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    static class TestNonComparable
    {
        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
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
            if (!e.getClass()
                    .isAssignableFrom(expected))
            {
                throw e;
            }

            assertTrue("Expected exception message to contain " + messageContains + " but was: " + e.getMessage(), e.getMessage()
                    .contains(messageContains));
        }
    }
}
