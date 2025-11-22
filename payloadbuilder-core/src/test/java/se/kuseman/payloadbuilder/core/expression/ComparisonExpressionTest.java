package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link ComparisonExpression} */
class ComparisonExpressionTest extends APhysicalPlanTest
{
    private static final IExpression NULL = new LiteralNullExpression(ResolvedType.of(Type.Boolean));
    private static final IExpression TRUE = LiteralBooleanExpression.TRUE;
    private static final IExpression FALSE = LiteralBooleanExpression.FALSE;

    @Test
    void test_fold()
    {
        IExpression e;

        e = e("1=1");
        assertTrue(e.isConstant());
        assertEquals(TRUE, e);
        e = e("1 > 1");
        assertEquals(FALSE, e);
        e = e("1=a");
        assertFalse(e.isConstant());
        assertEquals(e("1=a"), e);
        e = e("a=1");
        assertEquals(e("a=1"), e);
        assertFalse(e.isConstant());

        e = e("null=1");
        assertEquals(NULL, e);
        e = e("1=null");
        assertEquals(NULL, e);
        e = e("(1+2) > 10");
        assertEquals(FALSE, e);
        e = e("10 > (1+2)");
        assertEquals(TRUE, e);
        e = e("(1+2+a) > 10");
        assertEquals(e("(3+a) > 10"), e);
    }

    @Test
    void test_string()
    {
        // EQ
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.String, UTF8String.from("hello"), Type.Any, "hello", true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Any, "hello", Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Any, "hello", Type.String, UTF8String.from("world"), false, null));

        // NEQ
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.String, UTF8String.from("hello"), Type.Any, "hello", false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Any, "hello", Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Any, "hello", Type.String, UTF8String.from("world"), true, null));

        // GT
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.String, UTF8String.from("hello"), Type.Any, "hello", false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Any, "hello", Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Any, "world", Type.String, UTF8String.from("hello"), true, null));

        // GTE
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.String, UTF8String.from("hello"), Type.Any, "hello", true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Any, "hello", Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Any, "hello", Type.String, UTF8String.from("world"), false, null));

        // LT
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.String, UTF8String.from("hello"), Type.Any, "hello", false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Any, "hello", Type.String, UTF8String.from("hello"), false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Any, "hello", Type.String, UTF8String.from("world"), true, null));

        // LTE
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.String, UTF8String.from("hello"), Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.String, UTF8String.from("hello"), Type.Any, "hello", true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Any, "hello", Type.String, UTF8String.from("hello"), true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Any, "world", Type.String, UTF8String.from("hello"), false, null));
    }

    @Test
    void test_boolean()
    {
        // EQ
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, true, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, true, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, false, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, false, Type.Boolean, false, true, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, true, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, true, Type.Int, 0, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, false, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Boolean, false, Type.Int, 0, true, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Int, 1, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Int, 0, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Int, 1, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Int, 0, Type.Boolean, false, true, null));

        // NEQ
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, true, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, true, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, false, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, false, Type.Boolean, false, false, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, true, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, true, Type.Int, 0, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, false, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Boolean, false, Type.Int, 0, false, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Int, 1, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Int, 0, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Int, 1, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Int, 0, Type.Boolean, false, false, null));

        // GT
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, true, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, true, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, false, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, false, Type.Boolean, false, false, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, true, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, true, Type.Int, 0, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, false, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Boolean, false, Type.Int, 0, false, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 1, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 0, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 1, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 0, Type.Boolean, false, false, null));

        // GTE
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, true, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, true, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, false, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, false, Type.Boolean, false, true, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, true, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, true, Type.Int, 0, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, false, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Boolean, false, Type.Int, 0, true, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Int, 1, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Int, 0, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Int, 1, Type.Boolean, false, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Int, 0, Type.Boolean, false, true, null));

        // LT
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, true, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, true, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, false, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, false, Type.Boolean, false, false, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, true, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, true, Type.Int, 0, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, false, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, false, Type.Int, 0, false, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 1, Type.Boolean, true, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 0, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 1, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 0, Type.Boolean, false, false, null));

        // LTE
        // Bool <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, true, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, true, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, false, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, false, Type.Boolean, false, true, null));

        // Bool <> Int
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, true, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, true, Type.Int, 0, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, false, Type.Int, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Boolean, false, Type.Int, 0, true, null));

        // Int <> Bool
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Int, 1, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Int, 0, Type.Boolean, true, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Int, 1, Type.Boolean, false, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Int, 0, Type.Boolean, false, true, null));

        // Error
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.String, "test", Type.Boolean, false, true, "Cannot cast 'test' to Boolean"));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Boolean, true, Type.String, "test", true, "Cannot cast 'test' to Boolean"));
    }

    @Test
    void test_object()
    {
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Any, 1, Type.Any, 1, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Any, 1, Type.Any, 1F, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Any, 1, Type.Any, 0, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Any, 1, Type.Any, 1D, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN_EQUAL, Type.Any, 1, Type.Any, 1L, true, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 1, Type.Any, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN_EQUAL, Type.Any, 1, Type.Any, 1, true, null));

        // Test null
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Any, null, Type.Any, 1, null, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Any, 1, Type.Any, null, null, null));

    }

    @Test
    void test_numbers()
    {
        // 4 times 4 time 6 combinations = 96

        IComparisonExpression.Type[] ops = new IComparisonExpression.Type[] {
                IComparisonExpression.Type.EQUAL, IComparisonExpression.Type.NOT_EQUAL, IComparisonExpression.Type.GREATER_THAN, IComparisonExpression.Type.GREATER_THAN,
                IComparisonExpression.Type.LESS_THAN, IComparisonExpression.Type.LESS_THAN };
        Type[] types = new Type[] { Type.Int, Type.Long, Type.Float, Type.Double };
        Object[] values = new Object[] { 1, 2L, 3F, 4D };

        Map<IComparisonExpression.Type, Map<Type, boolean[]>> results = new HashMap<>();
        //@formatter:off
        results.put(IComparisonExpression.Type.EQUAL, ofEntries(
                // Int = Int,Long,Float,Double
                entry(Type.Int, new boolean[] { true, false, false, false }),
                // Long = Int,Long,Float,Double
                entry(Type.Long, new boolean[] { false, true, false, false }),
                // Float = Int,Long,Float,Double
                entry(Type.Float, new boolean[] { false, false, true, false }),
                // Double = Int,Long,Float,Double
                entry(Type.Double, new boolean[] { false, false, false, true })
                ));
        results.put(IComparisonExpression.Type.NOT_EQUAL, ofEntries(
                // Int != Int,Long,Float,Double
                entry(Type.Int, new boolean[] { false, true, true, true}),
                // Long != Int,Long,Float,Double
                entry(Type.Long, new boolean[] { true, false, true, true}),
                // Float != Int,Long,Float,Double
                entry(Type.Float, new boolean[] { true, true, false, true }),
                // Double != Int,Long,Float,Double
                entry(Type.Double, new boolean[] { true, true, true, false })
                ));
        results.put(IComparisonExpression.Type.GREATER_THAN, ofEntries(
                // Int > Int,Long,Float,Double
                entry(Type.Int, new boolean[] { false, false, false, false }),
                // Long > Int,Long,Float,Double
                entry(Type.Long, new boolean[] { true, false, false, false }),
                // Float > Int,Long,Float,Double
                entry(Type.Float, new boolean[] { true, true, false, false }),
                // Double > Int,Long,Float,Double
                entry(Type.Double, new boolean[] { true, true, true, false })
                ));
        results.put(IComparisonExpression.Type.GREATER_THAN_EQUAL, ofEntries(
                // Int >= Int,Long,Float,Double
                entry(Type.Int, new boolean[] { true, false, false, false }),
                // Long >= Int,Long,Float,Double
                entry(Type.Long, new boolean[] { true, true, false, false }),
                // Float >= Int,Long,Float,Double
                entry(Type.Float, new boolean[] { true, true, true, false }),
                // Double >= Int,Long,Float,Double
                entry(Type.Double, new boolean[] { true, true, true, true })
                ));
        results.put(IComparisonExpression.Type.LESS_THAN, ofEntries(
                // Int < Int,Long,Float,Double
                entry(Type.Int, new boolean[] { false, true, true, true }),
                // Long < Int,Long,Float,Double
                entry(Type.Long, new boolean[] { false, false, true, true}),
                // Float < Int,Long,Float,Double
                entry(Type.Float, new boolean[] { false, false, false, true }),
                // Double < Int,Long,Float,Double
                entry(Type.Double, new boolean[] { false, false, false, false })
                ));
        results.put(IComparisonExpression.Type.LESS_THAN_EQUAL, ofEntries(
                // Int <= Int,Long,Float,Double
                entry(Type.Int, new boolean[] { true, true, true, true }),
                // Long <= Int,Long,Float,Double
                entry(Type.Long, new boolean[] { false, true, true, true}),
                // Float <= Int,Long,Float,Double
                entry(Type.Float, new boolean[] { false, false, true, true }),
                // Double <= Int,Long,Float,Double
                entry(Type.Double, new boolean[] { false, false, false, true })
                ));
        //@formatter:on

        // Operations
        for (int i = 0; i < results.size(); i++)
        {
            Map<Type, boolean[]> opResults = results.get(ops[i]);
            for (int left = 0; left < 4; left++)
            {
                Type leftType = types[left];
                boolean[] typeResults = opResults.get(leftType);
                for (int right = 0; right < 4; right++)
                {
                    Type rightType = types[right];
                    TestCase test = new TestCase(ops[i], leftType, values[left], rightType, values[right], typeResults[right], null);
                    assertCase(test);
                }
            }
        }
    }

    @Test
    void test_numbers_int_false()
    {
        assertCase(new TestCase(IComparisonExpression.Type.EQUAL, Type.Int, 1, Type.Int, 0, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.NOT_EQUAL, Type.Int, 1, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 1, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.GREATER_THAN, Type.Int, 1, Type.Int, 10, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 1, Type.Int, 1, false, null));
        assertCase(new TestCase(IComparisonExpression.Type.LESS_THAN, Type.Int, 1, Type.Int, 0, false, null));
    }

    @Test
    void test_eq()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a = b
        e = new ComparisonExpression(IComparisonExpression.Type.EQUAL, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // a = null
        e = new ComparisonExpression(IComparisonExpression.Type.EQUAL, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null = a
        e = new ComparisonExpression(IComparisonExpression.Type.EQUAL, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    void test_neq()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a != b
        e = new ComparisonExpression(IComparisonExpression.Type.NOT_EQUAL, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // a != null
        e = new ComparisonExpression(IComparisonExpression.Type.NOT_EQUAL, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null != a
        e = new ComparisonExpression(IComparisonExpression.Type.NOT_EQUAL, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    void test_gt()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a > b
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // a > null
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null > a
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    void test_gte()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a >= b
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // a >= null
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null >= a
        e = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    void test_lt()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a < b
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false, false), actual);

        // a < null
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null < a
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    @Test
    void test_lte()
    {
        TupleVector tv = TupleVector.of(schema("col"), asList(vv(ResolvedType.of(Type.Any), 1, 2, 3, 4, 5)));

        ComparisonExpression e;
        ValueVector actual;

        // a <= b
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN_EQUAL, new LiteralIntegerExpression(5), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, true, true, true), actual);

        // a <= null
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN_EQUAL, new LiteralIntegerExpression(5), new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);

        // null <= a
        e = new ComparisonExpression(IComparisonExpression.Type.LESS_THAN_EQUAL, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralIntegerExpression(5));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), null, null, null, null, null), actual);
    }

    private void assertCase(TestCase test)
    {
        TupleVector input = TupleVector.of(schema(new Type[] { test.left, test.right }, "col1", "col2"),
                asList(vv(ResolvedType.of(test.left), test.leftValue), vv(ResolvedType.of(test.right), test.rightValue)));
        ComparisonExpression e = new ComparisonExpression(test.type, ce("col1"), ce("col2"));
        assertEquals(Type.Boolean, e.getType()
                .getType());
        try
        {
            ValueVector result = e.eval(input, context);
            assertEquals(Type.Boolean, result.type()
                    .getType());
            assertEquals(1, result.size());
            if (test.result == null)
            {
                assertTrue(result.isNull(0), test.toString());
            }
            else
            {
                assertEquals(test.result, result.getBoolean(0), test.toString());
            }

            if (test.assertExceptionMessage != null)
            {
                fail("Evaluation should fail with: " + test.assertExceptionMessage);
            }
        }
        catch (Exception ee)
        {
            if (test.assertExceptionMessage == null)
            {
                fail("Should not fail. " + ee.getMessage());
            }

            StringWriter sw = new StringWriter();
            ee.printStackTrace(new PrintWriter(sw));

            assertTrue(ee.getMessage()
                    .contains(test.assertExceptionMessage), "Should fail with message: " + test.assertExceptionMessage + " but was: " + sw.toString());
        }
    }

    /** Test case */
    public static class TestCase
    {
        private IComparisonExpression.Type type;
        private Type left;
        private Object leftValue;
        private Type right;
        private Object rightValue;
        private Object result;
        private String assertExceptionMessage;

        TestCase(IComparisonExpression.Type type, Type left, Object leftValue, Type right, Object rightValue, Object result, String assertExceptionMessage)
        {
            this.type = type;
            this.left = left;
            this.leftValue = leftValue;
            this.right = right;
            this.rightValue = rightValue;
            this.result = result;
            this.assertExceptionMessage = assertExceptionMessage;
        }

        @Override
        public String toString()
        {
            return String.format("%s [%s] %s %s [%s]", leftValue, left, type, rightValue, right);
        }
    }
}
