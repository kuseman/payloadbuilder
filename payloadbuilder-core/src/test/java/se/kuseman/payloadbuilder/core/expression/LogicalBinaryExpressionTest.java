package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link LogicalBinaryExpression} */
public class LogicalBinaryExpressionTest extends APhysicalPlanTest
{
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
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // true AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), bool(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);
    }

    @Test
    public void test_or_bitsetvector()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, bool(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
        assertEquals(Type.Boolean, e.getType()
                .getType());

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), bool(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);
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
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // true AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // true AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(true), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // false AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralBooleanExpression(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // false AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // false AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(false), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // null AND false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(ResolvedType.of(Type.Boolean)), new LiteralBooleanExpression(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // null AND true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(ResolvedType.of(Type.Boolean)), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null AND null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralNullExpression(ResolvedType.of(Type.Boolean)), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    public void test_or()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        LogicalBinaryExpression e;
        ValueVector actual;

        // true OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralBooleanExpression(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // true OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // true OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(true), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // false OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralBooleanExpression(false));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // false OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // false OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null OR false
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralBooleanExpression(false), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null OR true
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralNullExpression(ResolvedType.of(Type.Boolean)), new LiteralBooleanExpression(true));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // null OR null
        e = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.OR, new LiteralNullExpression(ResolvedType.of(Type.Boolean)), new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    private IExpression bool(boolean value)
    {
        return new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, new LiteralBooleanExpression(value), new LiteralBooleanExpression(value));
    }
}
