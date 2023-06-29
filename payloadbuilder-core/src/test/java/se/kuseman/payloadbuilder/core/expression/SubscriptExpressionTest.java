package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link SubscriptExpression} */
public class SubscriptExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_subscript_unsupported_type()
    {
        IExpression e = new SubscriptExpression(new LiteralDecimalExpression(Decimal.from(10)), new LiteralBooleanExpression(false));
        try
        {
            e.getType();
            fail("Should fail because of unsupported value");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Cannot subscript Decimal with Boolean"));
        }
    }

    @Test
    public void test_subscript_unsupported_type_eval()
    {
        IExpression e = new SubscriptExpression(new LiteralDecimalExpression(Decimal.from(10)), new LiteralBooleanExpression(false));
        try
        {
            e.eval(context);
            fail("Should fail because of unsupported value");
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Cannot subscript Decimal with Boolean"));
        }
    }

    @Test
    public void test_subscript_any_unsupported_value()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;

        input = ce("col2", ResolvedType.of(Type.Any));
        //@formatter:off
        tv = TupleVector.of(Schema.of(col("col", Type.Any), col("col2", Type.Any)),
                asList(
                        vv(ResolvedType.of(Type.Any), 1, 2),
                        vv(ResolvedType.of(Type.Any), true, false)
                        ));
        //@formatter:on

        try
        {
            expression = new SubscriptExpression(input, new LiteralIntegerExpression(0));
            expression.eval(tv, context);
            fail("Should fail becuase of unsupported runtime value");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript value: true"));
        }
    }

    @Test
    public void test_subscript_table_with_int()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;
        // CSOFF
        ValueVector actual;
        // CSON

        Schema innerSchema = Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Int)));

        input = ce("d", ResolvedType.table(innerSchema));
        //@formatter:off
        tv = TupleVector.of(Schema.of(col("col", Type.Any), col("col2", Type.Any), Column.of("d", ResolvedType.table(innerSchema))),
                asList(
                        vv(ResolvedType.of(Type.Any), 1, 2),
                        vv(ResolvedType.of(Type.Any), true, false),
                        vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 1,2,3),
                                vv(ResolvedType.of(Type.Int), 4,5,6))),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 10,20,30,40),
                                vv(ResolvedType.of(Type.Int), 40,50,60,50)))
                        )));
        //@formatter:on

        // Filter out row number 2 in all tuple vectors
        expression = new SubscriptExpression(input, intLit(2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.object(innerSchema), expression.getType());
        assertEquals(ResolvedType.object(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.object(innerSchema),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 3),
                        vv(ResolvedType.of(Type.Int), 6)))),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Filter out row number <last>-2 in all tuple vectors
        expression = new SubscriptExpression(input, intLit(-2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.object(innerSchema), expression.getType());
        assertEquals(ResolvedType.object(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.object(innerSchema),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 2),
                        vv(ResolvedType.of(Type.Int), 5)))),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Filter out row number that is larger than one of the tuple vectors
        expression = new SubscriptExpression(input, intLit(3));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.object(innerSchema), expression.getType());
        assertEquals(ResolvedType.object(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.object(innerSchema),
                null,
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 40),
                        vv(ResolvedType.of(Type.Int), 50))))
                ), actual);
        //@formatter:on

        // Filter out negative row number that is larger than one of the tuple vectors
        expression = new SubscriptExpression(input, intLit(-4));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.object(innerSchema), expression.getType());
        assertEquals(ResolvedType.object(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.object(innerSchema),
                null,
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 10),
                        vv(ResolvedType.of(Type.Int), 40))))
                ), actual);
        //@formatter:on

        // Test subscript value is null
        expression = new SubscriptExpression(input, new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.object(innerSchema), expression.getType());
        assertEquals(ResolvedType.object(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.object(innerSchema),
                null,
                null), actual);
        //@formatter:on

        // Test subscript value non typed
        expression = new SubscriptExpression(input, ce("col", ResolvedType.of(Type.Any)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 2),
                        vv(ResolvedType.of(Type.Int), 5)))),
                        ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Test subscript with compile time value that is not supported
        try
        {
            expression = new SubscriptExpression(input, LiteralBooleanExpression.FALSE);
            actual = expression.eval(tv, context);
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a Table with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(input, LiteralBooleanExpression.FALSE);
            expression.getType();
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a Table with Boolean"));
        }

        // Test subscript with runtime time value that is not supported
        try
        {
            expression = new SubscriptExpression(input, ce("col2", ResolvedType.of(Type.Any)));
            actual = expression.eval(tv, context);

            // Trig eval of value
            actual.toCsv();

            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a Table with value: true"));
        }
    }

    @Test
    public void test_subscript_table_with_string()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;
        // CSOFF
        ValueVector actual;
        // CSON

        Schema innerSchema = Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Int)));

        input = ce("d", ResolvedType.table(innerSchema));
        //@formatter:off
        tv = TupleVector.of(Schema.of(Column.of("d", ResolvedType.table(innerSchema)), Column.of("col3", ResolvedType.of(Type.Any))),
                asList(vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 1,2,3),
                                vv(ResolvedType.of(Type.Int), 4,5,6))),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 10,20,30),
                                vv(ResolvedType.of(Type.Int), 40,50,60)))
                        ),
                        vv(ResolvedType.of(Type.Any), "col1", UTF8String.from("col2"))));
        //@formatter:on

        // Filter out row number 2 in all tuple vectors
        expression = new SubscriptExpression(input, new LiteralStringExpression("COL1"));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), expression.getType());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.of(Type.Any)),
                vv(ResolvedType.of(Type.Int), 1,2,3),
                vv(ResolvedType.of(Type.Int), 10,20,30)
                ), actual);
        //@formatter:on

        // Filter out columns with non existent column
        expression = new SubscriptExpression(input, new LiteralStringExpression("COL4"));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), expression.getType());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.of(Type.Any)),
                null,
                null
                ), actual);
        //@formatter:on

        // Filter out columns with Any type
        expression = new SubscriptExpression(input, ce("col3"));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                vv(ResolvedType.of(Type.Int), 1,2,3),
                vv(ResolvedType.of(Type.Int), 40,50,60)
                ), actual);
        //@formatter:on
    }

    @Test
    public void test_subscript_any_table_with_int()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;
        // CSOFF
        ValueVector actual;
        // CSON

        Schema innerSchema = Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Int)));

        input = ce("d", ResolvedType.of(Type.Any));
        //@formatter:off
        tv = TupleVector.of(Schema.of(col("col", Type.Any), col("col2", Type.Any), Column.of("d", ResolvedType.of(Type.Any))),
                asList(
                        vv(ResolvedType.of(Type.Any), 1, 2),
                        vv(ResolvedType.of(Type.Any), true, false),
                        vv(ResolvedType.of(Type.Any),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 1,2,3),
                                vv(ResolvedType.of(Type.Int), 4,5,6))),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 10,20,30,40),
                                vv(ResolvedType.of(Type.Int), 40,50,60,50)))
                        )));
        //@formatter:on

        // Filter out row number 2 in all tuple vectors
        expression = new SubscriptExpression(input, intLit(2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 3),
                        vv(ResolvedType.of(Type.Int), 6)))),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Filter out row number <last>-2 in all tuple vectors
        expression = new SubscriptExpression(input, intLit(-2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 2),
                        vv(ResolvedType.of(Type.Int), 5)))),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Filter out row number that is larger than one of the tuple vectors
        expression = new SubscriptExpression(input, intLit(3));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                null,
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 40),
                        vv(ResolvedType.of(Type.Int), 50))))
                ), actual);
        //@formatter:on

        // Filter out negative row number that is larger than one of the tuple vectors
        expression = new SubscriptExpression(input, intLit(-4));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                null,
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 10),
                        vv(ResolvedType.of(Type.Int), 40))))
                ), actual);
        //@formatter:on

        // Test subscript value is null
        expression = new SubscriptExpression(input, new LiteralNullExpression(ResolvedType.of(Type.Int)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                null,
                null), actual);
        //@formatter:on

        // Test subscript value non typed
        expression = new SubscriptExpression(input, ce("col", ResolvedType.of(Type.Any)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 2),
                        vv(ResolvedType.of(Type.Int), 5)))),
                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60))))
                ), actual);
        //@formatter:on

        // Test subscript with compile time value that is not supported
        try
        {
            expression = new SubscriptExpression(input, LiteralBooleanExpression.FALSE);
            actual = expression.eval(tv, context);
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a Table with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(input, LiteralBooleanExpression.FALSE);
            expression.getType();
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript Any with Boolean"));
        }

        // Test subscript with runtime time value that is not supported
        try
        {
            expression = new SubscriptExpression(input, ce("col2", ResolvedType.of(Type.Any)));
            actual = expression.eval(tv, context);

            // Trig eval of value
            actual.toCsv();

            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a Table with value: true"));
        }
    }

    @Test
    public void test_subscript_any_array_with_int()
    {
        IExpression expression;
        TupleVector tv;
        ValueVector actual;

        //@formatter:off
        tv = TupleVector.of(Schema.of(
                col("col", Type.Any),
                col("col1", Type.Any),
                Column.of("col2", Type.Any)
                ),
                asList(
                    vv(ResolvedType.of(Type.Any), 1,null,3),
                    vv(ResolvedType.of(Type.Any),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value"), MapUtils.entry("key1", 123)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value10"), MapUtils.entry("key1", 1230)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value100"), MapUtils.entry("key1", 12300))), 
                    vv(ResolvedType.of(Type.Any),
                            vv(ResolvedType.of(Type.Int), 4,5,6),
                            null,
                            asList(17, 18, 19)  // Runtime cast to value vector
                            )
                    ));
        //@formatter:on

        // Subscript with int values on index 2
        expression = new SubscriptExpression(ce("col2"), intLit(2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 6, null, 19), actual);

        // Subscript with int values on index -2
        expression = new SubscriptExpression(ce("col2"), intLit(-2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, null, 18), actual);

        // Subscript with int values on negative index larger than array size
        expression = new SubscriptExpression(ce("col2"), intLit(-10));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, null, null), actual);

        // Subscript with Any values
        expression = new SubscriptExpression(ce("col2"), ce("col", ResolvedType.of(Type.Any)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 5, null, null), actual);

        // Subscript with Any values not supported
        try
        {
            expression = new SubscriptExpression(ce("col2"), ce("col1", ResolvedType.of(Type.Any)));
            actual = expression.eval(tv, context);
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript an Array with value: {key=value, key1=123}"));
        }

        // Test subscript with compile time value that is not supported
        try
        {
            expression = new SubscriptExpression(ce("col2"), LiteralBooleanExpression.FALSE);
            actual = expression.eval(tv, context);
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript an Array with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(ce("col2"), LiteralBooleanExpression.FALSE);
            expression.getType();
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript Any with Boolean"));
        }
    }

    @Test
    public void test_subscript_any_string_with_int()
    {
        IExpression expression;
        TupleVector tv;
        ValueVector actual;

        //@formatter:off
        tv = TupleVector.of(Schema.of(
                col("col", Type.Any),
                col("col1", Type.Int),
                col("col3", Type.Any)
                ),
                asList(
                    vv(ResolvedType.of(Type.Any), 1, null, 3, -1, 10, -10),
                    vv(ResolvedType.of(Type.Int), 1, null, 3, -1, 10, -10),
                    vv(ResolvedType.of(Type.Any), "hello", UTF8String.from("hello"), null, "hello", UTF8String.from("hello"), "hello")
                    ));
        //@formatter:on

        // Any -> Any
        expression = new SubscriptExpression(ce("col3"), ce("col"));
        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        actual = expression.eval(tv, context);

        assertVectorsEquals(vv(Type.Any, "e", null, null, "o", null, null), actual);

        // Any -> Int
        expression = new SubscriptExpression(ce("col3"), ce("col1"));
        actual = expression.eval(tv, context);

        assertVectorsEquals(vv(Type.Any, "e", null, null, "o", null, null), actual);

        // Unsupported subscript type (Any -> boolean)
        try
        {
            expression = new SubscriptExpression(ce("col3"), new LiteralBooleanExpression(false));
            actual = expression.eval(tv, context);
            fail("Should fail because of unsupported subscript type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a String with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(ce("col3"), new LiteralBooleanExpression(false));
            expression.getType();
            fail("Should fail because of unsupported subscript type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript Any with Boolean"));
        }
    }

    @Test
    public void test_subscript_string_with_int()
    {
        IExpression expression;
        TupleVector tv;
        ValueVector actual;

        //@formatter:off
        tv = TupleVector.of(Schema.of(
                col("col", Type.Any),
                col("col1", Type.Int),
                col("col2", Type.Any)
                ),
                asList(
                    vv(ResolvedType.of(Type.Any), 1, null, 3, -1, 10, -10),
                    vv(ResolvedType.of(Type.Int), 1, null, 3, -1, 10, -10),
                    vv(ResolvedType.of(Type.Any), true, false, true, false, true, true)
                    ));
        //@formatter:on

        // Any
        expression = new SubscriptExpression(new LiteralStringExpression("hello"), ce("col"));
        assertEquals(ResolvedType.of(Type.String), expression.getType());
        actual = expression.eval(tv, context);

        assertVectorsEquals(vv(Type.String, "e", null, "l", "o", null, null), actual);

        // Int
        expression = new SubscriptExpression(new LiteralStringExpression("hello"), ce("col1"));
        actual = expression.eval(tv, context);

        assertVectorsEquals(vv(Type.String, "e", null, "l", "o", null, null), actual);

        // Unsupported subscript type
        try
        {
            expression = new SubscriptExpression(new LiteralStringExpression("hello"), new LiteralBooleanExpression(false));
            actual = expression.eval(tv, context);
            fail("Should fail because of unsupported subscript type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a String with Boolean"));
        }

        // Unsupported subscript type Any
        try
        {
            expression = new SubscriptExpression(new LiteralStringExpression("hello"), ce("col2"));
            actual = expression.eval(tv, context);
            fail("Should fail because of unsupported subscript type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a String with value: true"));
        }

        try
        {
            expression = new SubscriptExpression(new LiteralStringExpression("hello"), new LiteralBooleanExpression(false));
            expression.getType();
            fail("Should fail because of unsupported subscript type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a String with Boolean"));
        }
    }

    @Test
    public void test_subscript_array_with_int()
    {
        IExpression expression;
        TupleVector tv;
        ValueVector actual;

        //@formatter:off
        tv = TupleVector.of(Schema.of(
                col("col", Type.Any),
                col("col1", Type.Any),
                Column.of("col2", ResolvedType.array(ResolvedType.of(Type.Int)))
                ),
                asList(
                    vv(ResolvedType.of(Type.Any), 1,null,3),
                    vv(ResolvedType.of(Type.Any),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value"), MapUtils.entry("key1", 123)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value10"), MapUtils.entry("key1", 1230)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value100"), MapUtils.entry("key1", 12300))), 
                    vv(ResolvedType.array(ResolvedType.of(Type.Int)),
                            vv(ResolvedType.of(Type.Int), 4,5,6),
                            null,
                            vv(ResolvedType.of(Type.Int), 17,18,19)
                            )
                    ));
        //@formatter:on

        // Subscript with int values on index 2
        expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), intLit(2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, null, 19), actual);

        // Subscript with int values on index -2
        expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), intLit(-2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 5, null, 18), actual);

        // Subscript with int values on negative index larger than array size
        expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), intLit(-10));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), null, null, null), actual);

        // Subscript with Any values
        expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), ce("col", ResolvedType.of(Type.Any)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 5, null, null), actual);

        // Subscript with Any values not supported
        try
        {
            expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), ce("col1", ResolvedType.of(Type.Any)));
            actual = expression.eval(tv, context);
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript an Array with value: {key=value, key1=123}"));
        }

        // Test subscript with compile time value that is not supported
        try
        {
            expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), LiteralBooleanExpression.FALSE);
            actual = expression.eval(tv, context);
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript an Array with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(ce("col2", ResolvedType.array(ResolvedType.of(Type.Any))), LiteralBooleanExpression.FALSE);
            expression.getType();
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript an Array with Boolean"));
        }
    }
}
