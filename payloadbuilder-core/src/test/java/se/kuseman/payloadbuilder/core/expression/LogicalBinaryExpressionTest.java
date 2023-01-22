package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.BitSetVector;

/** Test of {@link LogicalBinaryExpression} */
public class LogicalBinaryExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_vector_size_1()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Boolean }, "col"), asList(ValueVector.literalBoolean(false, 5)));
        LogicalBinaryExpression e;
        ValueVector actual;

        // Construct a literal bool with a fixed size 1
        LiteralBooleanExpression litBool = new LiteralBooleanExpression(false)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                return ValueVector.literalBoolean(true, 1);
            }
        };

        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, litBool, ce("col"));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, ce("col"), litBool);
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // Test that size != 1 fails
        LiteralBooleanExpression litBool1 = new LiteralBooleanExpression(false)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                return ValueVector.literalBoolean(true, 3);
            }
        };
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, ce("col"), litBool1);

        try
        {
            e.eval(tv, context);
            fail("Should fail with different sizes");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Evaluation of binary vectors requires equal size"));
        }
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_error_non_boolean()
    {
        new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralIntegerExpression(4)).eval(TupleVector.EMPTY, null);
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_error_non_boolean_1()
    {
        new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralIntegerExpression(4), new LiteralIntegerExpression(5)).eval(TupleVector.EMPTY, null);
    }

    @Test
    public void test_and_bitsetvector()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, bool(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertTrue(actual instanceof BitSetVector);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // true AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), bool(false));
        actual = e.eval(tv, null);
        assertTrue(actual instanceof BitSetVector);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());
    }

    @Test
    public void test_or_bitsetvector()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, bool(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertTrue(actual instanceof BitSetVector);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());
        assertEquals(Type.Boolean, e.getType()
                .getType());

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), bool(false));
        actual = e.eval(tv, null);
        assertTrue(actual instanceof BitSetVector);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());
        assertEquals(Type.Boolean, e.getType()
                .getType());
    }

    @Test
    public void test_and()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // true AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // true AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());

        // false AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // false AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // false AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // null AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(Type.Boolean), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // null AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(Type.Boolean), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());

        // null AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(Type.Boolean), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());
    }

    @Test
    public void test_or()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // true OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // true OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // false OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralBooleanExpression(false));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
        assertFalse(actual.isNullable());

        // false OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // false OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());

        // null OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());

        // null OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralNullExpression(Type.Boolean), new LiteralBooleanExpression(true));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertFalse(actual.isNullable());

        // null OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralNullExpression(Type.Boolean), new LiteralNullExpression(Type.Boolean));
        actual = e.eval(tv, null);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
        assertTrue(actual.isNullable());
    }

    private IExpression bool(boolean value)
    {
        return new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(value), new LiteralBooleanExpression(value));
    }
}
