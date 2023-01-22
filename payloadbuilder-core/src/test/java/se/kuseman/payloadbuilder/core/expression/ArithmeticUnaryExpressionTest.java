package se.kuseman.payloadbuilder.core.expression;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.expression.IArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Test of {@link ArithmeticUnaryExpression} */
public class ArithmeticUnaryExpressionTest extends AExpressionTest
{
    private static final IExpression NULL = new LiteralNullExpression(ResolvedType.of(Type.Boolean));

    @Test
    public void test_fold()
    {
        IExpression e;

        e = e("-(1+2)");
        assertTrue(e.isConstant());
        assertEquals(e("-3"), e);

        e = e("-null");
        assertEquals(NULL, e);

        e = e("-a");
        assertFalse(e.isConstant());
        assertEquals(e("-a"), e);

        e = e("-(1+2+a)");
        assertEquals(e("-(3+a)"), e);

        e = e("-(-2L)");
        assertEquals(e("2L"), e);

        e = e("-(-2F)");
        assertEquals(e("2F"), e);

        e = e("-(-2D)");
        assertEquals(e("2D"), e);

        e = new ArithmeticUnaryExpression(IArithmeticUnaryExpression.Type.MINUS, new LiteralDecimalExpression(Decimal.from(10))).fold();
        assertEquals(new LiteralDecimalExpression(Decimal.from(-10)), e);
    }
}
