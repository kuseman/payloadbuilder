package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.Ignore;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link ArithmeticBinaryExpression} */
public class ArithmeticBinaryExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_semantic_equals()
    {
        IExpression left = e("a");
        IExpression right = e("b");
        IExpression c = e("c");

        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);

        assertTrue(e.semanticEquals(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right)));
        assertTrue(e.semanticEquals(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, right, left)));
        assertFalse(e.semanticEquals(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.SUBTRACT, right, left)));
        assertFalse(e.semanticEquals(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, c)));
        assertFalse(e.semanticEquals(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, c, left)));
        assertFalse(e.semanticEquals(intLit(10)));
    }

    @Test
    public void test_vector_size_1()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(ValueVector.literalInt(10, 5)));
        ArithmeticBinaryExpression e;
        ValueVector actual;

        // Construct a literal int with a fixed size 1
        LiteralIntegerExpression litInt = new LiteralIntegerExpression(0)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                return ValueVector.literalInt(5, 1);
            }
        };

        e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, litInt, ce("col"));
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 15, 15, 15, 15, 15), actual);

        e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, ce("col"), litInt);
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 15, 15, 15, 15, 15), actual);

        // Test that size != 1 fails
        LiteralIntegerExpression litInt2 = new LiteralIntegerExpression(0)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                return ValueVector.literalInt(5, 2);
            }
        };
        e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, ce("col"), litInt2);

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

    @Test
    public void test_exceptions()
    {
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Boolean, true, Type.String, 10, null, null, "Cannot perform arithmetics on types Boolean and String"));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, 10, Type.Boolean, true, null, null, "Cannot perform arithmetics on types String and Boolean"));
    }

    @Test
    public void test_cases_implicit_cast()
    {
        List<TestCase> cases = new ArrayList<>();

        // Add

        // Boolean
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Boolean, true, Type.Int, 10, Type.Int, 11, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Boolean, false, Type.Long, 20L, Type.Long, 20L, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Boolean, true, Type.Float, 30F, Type.Float, 31F, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Boolean, false, Type.Double, 40D, Type.Double, 40D, null));

        // String (conversion fails)
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "abc", Type.Int, 10, Type.Int, 11, "Cannot cast 'abc' to Int"));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "def", Type.Long, 10, Type.Long, 11, "Cannot cast 'def' to Long"));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "ghj", Type.Float, 10, Type.Float, 11, "Cannot cast 'ghj' to Float"));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "klm", Type.Double, 10, Type.Double, 11, "Cannot cast 'klm' to Double"));

        // String (ints)
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "1", Type.Int, 10, Type.Int, 11, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "1", Type.Long, 20L, Type.Long, 21L, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "1", Type.Float, 30F, Type.Float, 31F, null));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "1", Type.Double, 40D, Type.Double, 41D, null));

        // String (decimals)
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "3.1", Type.Float, 30F, Type.Float, 33.1F, null, false));
        cases.add(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.String, "4.1", Type.Double, 40D, Type.Double, 44.1D, null, false));

        for (TestCase c : cases)
        {
            assertCase(c);
        }
    }

    @Test
    public void test_cases()
    {
        IArithmeticBinaryExpression.Type[] ops = new IArithmeticBinaryExpression.Type[] {
                IArithmeticBinaryExpression.Type.ADD, IArithmeticBinaryExpression.Type.SUBTRACT, IArithmeticBinaryExpression.Type.MULTIPLY, IArithmeticBinaryExpression.Type.DIVIDE,
                IArithmeticBinaryExpression.Type.MODULUS };
        Type[] types = new Type[] { Type.Int, Type.Long, Type.Float, Type.Double };
        Object[] values = new Object[] { 1, 2L, 3F, 4D };

        Map<IArithmeticBinaryExpression.Type, Map<Type, Object[]>> results = new HashMap<>();
        //@formatter:off
        results.put(IArithmeticBinaryExpression.Type.ADD, ofEntries(
                // Int + Int,Long,Float,Double
                entry(Type.Int, new Object[] { Type.Int, 2, Type.Long, 3L, Type.Float, 4F, Type.Double, 5D }),
                // Long + Int,Long,Float,Double
                entry(Type.Long, new Object[] { Type.Long, 3L, Type.Long, 4L, Type.Float, 5.0F, Type.Double, 6.0D }),
                // Float + Int,Long,Float,Double
                entry(Type.Float, new Object[] { Type.Float, 4.0F, Type.Float, 5.0F, Type.Float, 6.0F, Type.Double, 7.0D }),
                // Double + Int,Long,Float,Double
                entry(Type.Double, new Object[] { Type.Double, 5.0D, Type.Double, 6.0D, Type.Double, 7.0D, Type.Double, 8.0D })
                ));
        results.put(IArithmeticBinaryExpression.Type.SUBTRACT, ofEntries(
                // Int - Int,Long,Float,Double
                entry(Type.Int, new Object[] { Type.Int, 0, Type.Long, -1L, Type.Float, -2.0F, Type.Double, -3.0D }),
                // Long - Int,Long,Float,Double
                entry(Type.Long, new Object[] { Type.Long, 1L, Type.Long, 0L, Type.Float, -1.0F, Type.Double, -2.0D }),
                // Float - Int,Long,Float,Double
                entry(Type.Float, new Object[] { Type.Float, 2.0F, Type.Float, 1.0F, Type.Float, 0.0F, Type.Double, -1.0D }),
                // Double - Int,Long,Float,Double
                entry(Type.Double, new Object[] { Type.Double, 3.0D, Type.Double, 2.0D, Type.Double, 1.0D, Type.Double, 0.0D })
                ));
        results.put(IArithmeticBinaryExpression.Type.MULTIPLY, ofEntries(
                // Int * Int,Long,Float,Double
                entry(Type.Int, new Object[] { Type.Int, 1, Type.Long, 2L, Type.Float, 3.0F, Type.Double, 4.0D }),
                // Long * Int,Long,Float,Double
                entry(Type.Long, new Object[] { Type.Long, 2L, Type.Long, 4L, Type.Float, 6.0F, Type.Double, 8.0D }),
                // Float * Int,Long,Float,Double
                entry(Type.Float, new Object[] { Type.Float, 3.0F, Type.Float, 6.0F, Type.Float, 9.0F, Type.Double, 12.0D }),
                // Double * Int,Long,Float,Double
                entry(Type.Double, new Object[] { Type.Double, 4.0D, Type.Double, 8.0D, Type.Double, 12.0D, Type.Double, 16.0D })
                ));
        results.put(IArithmeticBinaryExpression.Type.DIVIDE, ofEntries(
                // Int / Int,Long,Float,Double
                entry(Type.Int, new Object[] { Type.Int, 1, Type.Long, 0L, Type.Float, 0.3333F, Type.Double, 0.25D }),
                // Long / Int,Long,Float,Double
                entry(Type.Long, new Object[] { Type.Long, 2L, Type.Long, 1L, Type.Float, 0.666F, Type.Double, 0.5D }),
                // Float / Int,Long,Float,Double
                entry(Type.Float, new Object[] { Type.Float, 3.0F, Type.Float, 1.5F, Type.Float, 1.0F, Type.Double, 0.75D }),
                // Double / Int,Long,Float,Double
                entry(Type.Double, new Object[] { Type.Double, 4.0D, Type.Double, 2.0D, Type.Double, 1.3333D, Type.Double, 1.0D })
                ));
        results.put(IArithmeticBinaryExpression.Type.MODULUS, ofEntries(
                // Int % Int,Long,Float,Double
                entry(Type.Int, new Object[] { Type.Int, 0, Type.Long, 1L, Type.Float, 1.0F, Type.Double, 1.0D }),
                // Long % Int,Long,Float,Double
                entry(Type.Long, new Object[] { Type.Long, 0L, Type.Long, 0L, Type.Float, 2.0F, Type.Double, 2.0D }),
                // Float % Int,Long,Float,Double
                entry(Type.Float, new Object[] { Type.Float, 0.0F, Type.Float, 1.0F, Type.Float, 0.0F, Type.Double, 3.0D }),
                // Double % Int,Long,Float,Double
                entry(Type.Double, new Object[] { Type.Double, 0.0D, Type.Double, 0.0D, Type.Double, 1.0D, Type.Double, 0.0D })
                ));
        //@formatter:on

        // Operations
        for (int i = 0; i < results.size(); i++)
        {
            Map<Type, Object[]> opResults = results.get(ops[i]);
            for (int left = 0; left < 4; left++)
            {
                Type leftType = types[left];
                Object[] typeResults = opResults.get(leftType);
                for (int right = 0; right < 4; right++)
                {
                    Type resultType = (Type) typeResults[right * 2];
                    Object result = typeResults[(right * 2) + 1];

                    Type rightType = types[right];
                    TestCase test = new TestCase(ops[i], leftType, values[left], rightType, values[right], resultType, result, null);
                    assertCase(test);
                }
            }
        }
    }

    @Test
    public void test_null()
    {
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Int, null, Type.Float, 1.0F, Type.Float, null, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Int, 1, Type.Float, null, Type.Float, null, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Int, null, Type.Float, null, Type.Float, null, null));
    }

    @Test
    public void test_any()
    {
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.ADD, Type.Any, 1, Type.Any, 1.0F, Type.Any, 2.0F, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.DIVIDE, Type.Any, 2L, Type.Any, 2L, Type.Any, 1L, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.MODULUS, Type.Any, 3F, Type.Any, 3D, Type.Any, 0D, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.MULTIPLY, Type.Any, 4D, Type.Any, 4F, Type.Any, 16.0D, null));
        assertCase(new TestCase(IArithmeticBinaryExpression.Type.SUBTRACT, Type.Any, 5L, Type.Any, 5, Type.Any, 0L, null));
    }

    @Test
    public void test_int_add()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5)));

        IExpression left = ce("col", ResolvedType.of(Type.Int));
        LiteralIntegerExpression right = new LiteralIntegerExpression(200);

        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);

        assertEquals(Type.Int, e.getType()
                .getType());

        ValueVector actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 201, 202, 203, 204, 205), actual);
    }

    @Test
    public void test_add_int_and_float()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5)));

        IExpression left = ce("col", ResolvedType.of(Type.Int));
        LiteralFloatExpression right = new LiteralFloatExpression(10.10F);

        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);

        assertEquals(Type.Float, e.getType()
                .getType());

        ValueVector actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 11.1F, 12.1F, 13.1F, 14.1F, 15.1F), actual);
    }

    @Test
    public void test_add_int_and_double()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5)));

        IExpression left = ce("col", ResolvedType.of(Type.Int));
        LiteralDoubleExpression right = new LiteralDoubleExpression(10.10D);

        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);

        assertEquals(Type.Double, e.getType()
                .getType());

        ValueVector actual = e.eval(tv, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Double), 11.1D, 12.1D, 13.1D, 14.1D, 15.1D), actual);
    }

    @Test(
            expected = ArithmeticException.class)
    public void test_int_add_overflow()
    {
        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5)));

        IExpression left = ce("col", ResolvedType.of(Type.Int));
        LiteralIntegerExpression right = new LiteralIntegerExpression(Integer.MAX_VALUE);
        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, left, right);
        assertEquals(Type.Int, e.getType()
                .getType());

        ValueVector actual = e.eval(tv, context);
        assertEquals(10, actual.getInt(1)); // Triggers overflow
    }

    private void assertCase(TestCase test)
    {
        TupleVector input = TupleVector.of(schema(new Type[] { test.left, test.right }, "col1", "col2"),
                asList(vv(ResolvedType.of(test.left), test.leftValue), vv(ResolvedType.of(test.right), test.rightValue)));
        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(test.op, ce("col1"), ce("col2"));

        try
        {
            ValueVector result = e.eval(input, context);
            assertEquals(test.toString(), ResolvedType.of(test.resultType), result.type());
            assertEquals(test.toString(), 1, result.size());
            if (test.result == null)
            {
                assertTrue(test.toString(), result.isNull(0));
            }
            else
            {
                assertVectorsEquals(test.toString(), vv(ResolvedType.of(test.resultType), test.result), result, test.performNumberCasts);
            }

            // Fail the test if not already failed
            if (test.assertExceptionMessage != null)
            {
                fail("Evaluation should fail with: " + test.assertExceptionMessage);
            }
        }
        catch (Exception ee)
        {
            if (test.assertExceptionMessage == null)
            {
                StringWriter sw = new StringWriter();
                ee.printStackTrace(new PrintWriter(sw));

                fail("Should not fail. " + test.toString() + "\n" + sw.toString());
            }

            assertTrue("Should fail with message: " + test.assertExceptionMessage + " but was: " + ee.getMessage(), ee.getMessage()
                    .contains(test.assertExceptionMessage));
        }
    }

    /** Test case */
    public static class TestCase
    {
        private IArithmeticBinaryExpression.Type op;
        private Type left;
        private Object leftValue;
        private Type right;
        private Object rightValue;
        private Type resultType;
        private Object result;
        private String assertExceptionMessage;
        private boolean performNumberCasts;

        TestCase(IArithmeticBinaryExpression.Type op, Type left, Object leftValue, Type right, Object rightValue, Type resultType, Object result, String assertExceptionMessage)
        {
            this(op, left, leftValue, right, rightValue, resultType, result, assertExceptionMessage, true);
        }

        TestCase(IArithmeticBinaryExpression.Type op, Type left, Object leftValue, Type right, Object rightValue, Type resultType, Object result, String assertExceptionMessage,
                boolean performNumberCasts)
        {
            this.op = op;
            this.left = left;
            this.leftValue = leftValue;
            this.right = right;
            this.rightValue = rightValue;
            this.resultType = resultType;
            this.result = result;
            this.assertExceptionMessage = assertExceptionMessage;
            this.performNumberCasts = performNumberCasts;
        }

        @Override
        public String toString()
        {
            return String.format("%s: %s [%s] %s %s [%s]", resultType, leftValue, left, op, rightValue, right);
        }
    }

    @Ignore
    @Test
    public void measure()
    {
        Random r = new Random(System.nanoTime());
        final int[] numbers = new int[100_000_000];
        for (int i = 0; i < numbers.length; i++)
        {
            numbers[i] = r.nextInt(100);
        }

        ValueVector vv = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                // Test grouping primitive vs instance
                return ResolvedType.of(Type.Int);
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return numbers.length;
            }

            @Override
            public Object getValue(int row)
            {
                return numbers[row];
            }

            @Override
            public int getInt(int row)
            {
                return numbers[row];
            }
        };

        TupleVector tv = TupleVector.of(schema(new Type[] { Type.Int }, "col"), asList(vv));

        IExpression left = ce("col");
        LiteralIntegerExpression right = new LiteralIntegerExpression(100);
        ArithmeticBinaryExpression e = new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.MULTIPLY, left, right);

        for (int i = 0; i < 10_00000; i++)
        {
            long start = System.nanoTime();

            ValueVector c = e.eval(tv, context);
            // assertTrue(c.size() > 0);

            long sum = 0;
            for (int j = 0; j < c.size(); j++)
            {
                sum += c.getInt(j);
            }

            assertTrue(sum > 0);
            // System.out.println(actual.toCsv());
            System.out.println(DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }

    }
}
