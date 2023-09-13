package se.kuseman.payloadbuilder.core.physicalplan;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;

/** Test of {@link Decimal} */
public class DecimalTest extends org.junit.Assert
{
    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void test()
    {
        Decimal d = Decimal.from("10.10");

        assertTrue(d.equals(d));
        assertFalse(d.equals(Decimal.from("10.100")));
        assertFalse(d.equals(null));
        assertFalse(d.equals("helloe"));

        assertEquals(Decimal.from("-10.10"), d.negate());

        assertEquals(0, d.compareTo(Decimal.from("10.100")));
        assertEquals(-1, d.compareTo(Decimal.from("10.101")));
        assertEquals(1, d.compareTo(Decimal.from("9.101")));

        assertEquals(Decimal.from("10.20"), d.processArithmetic(Decimal.from("0.10"), IArithmeticBinaryExpression.Type.ADD));
        assertEquals(Decimal.from("101.00"), d.processArithmetic(Decimal.from("0.10"), IArithmeticBinaryExpression.Type.DIVIDE));
        assertEquals(Decimal.from("1.70"), d.processArithmetic(Decimal.from("2.10"), IArithmeticBinaryExpression.Type.MODULUS));
        assertEquals(Decimal.from("1.0100"), d.processArithmetic(Decimal.from("0.10"), IArithmeticBinaryExpression.Type.MULTIPLY));
        assertEquals(Decimal.from("9.90"), d.processArithmetic(Decimal.from("0.20"), IArithmeticBinaryExpression.Type.SUBTRACT));
    }
}
