package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.nvv;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link SubscriptExpression} */
public class SubscriptExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_tuplevector_integer_subscript()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;
        // CSOFF
        ValueVector actual;
        // CSON

        Schema innerSchema = Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Int)));

        input = ce("d", ResolvedType.tupleVector(innerSchema));
        //@formatter:off
        tv = TupleVector.of(Schema.of(col("col", Type.Any), col("col2", Type.Any), Column.of("d", ResolvedType.tupleVector(innerSchema))),
                asList(
                        vv(ResolvedType.of(Type.Any), 1, 2), 
                        vv(ResolvedType.of(Type.Any), true, false), 
                        vv(ResolvedType.tupleVector(innerSchema), 
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

        assertEquals(ResolvedType.tupleVector(innerSchema), expression.getType());
        assertEquals(ResolvedType.tupleVector(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.tupleVector(innerSchema),
                TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 3),
                        vv(ResolvedType.of(Type.Int), 6))),
                TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60)))
                ), actual);
        //@formatter:on

        // Filter out row number that is larger than one of the tuple vectors
        expression = new SubscriptExpression(input, intLit(3));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.tupleVector(innerSchema), expression.getType());
        assertEquals(ResolvedType.tupleVector(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.tupleVector(innerSchema),
                null,
                TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 40),
                        vv(ResolvedType.of(Type.Int), 50)))
                ), actual);
        //@formatter:on

        // Test subscript value is null
        expression = new SubscriptExpression(input, new LiteralNullExpression(Type.Int));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.tupleVector(innerSchema), expression.getType());
        assertEquals(ResolvedType.tupleVector(innerSchema), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.tupleVector(innerSchema),
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
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any),
                TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 2),
                        vv(ResolvedType.of(Type.Int), 5))),
                TupleVector.of(innerSchema, asList(
                        vv(ResolvedType.of(Type.Int), 30),
                        vv(ResolvedType.of(Type.Int), 60)))
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
                    .contains("Cannot subscript a TupleVector with Boolean"));
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
                    .contains("Cannot subscript a TupleVector with Boolean"));
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
                    .contains("Cannot subscript a TupleVector with value: true"));
        }
    }

    @Test
    public void test_tuplevector_string_subscript()
    {
        IExpression expression;
        IExpression input;
        TupleVector tv;
        // CSOFF
        ValueVector actual;
        // CSON

        Schema innerSchema = Schema.of(Column.of("col1", ResolvedType.of(Type.Int)), Column.of("col2", ResolvedType.of(Type.Int)));

        input = ce("d", ResolvedType.tupleVector(innerSchema));
        //@formatter:off
        tv = TupleVector.of(Schema.of(Column.of("d", ResolvedType.tupleVector(innerSchema))),
                asList(vv(ResolvedType.tupleVector(innerSchema), 
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 1,2,3),
                                vv(ResolvedType.of(Type.Int), 4,5,6))),
                        TupleVector.of(innerSchema, asList(
                                vv(ResolvedType.of(Type.Int), 10,20,30),
                                vv(ResolvedType.of(Type.Int), 40,50,60)))
                        )));
        //@formatter:on

        // Filter out row number 2 in all tuple vectors
        expression = new SubscriptExpression(input, new LiteralStringExpression("COL1"));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), expression.getType());
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), actual.type());
        assertEquals(2, actual.size());

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.valueVector(ResolvedType.of(Type.Any)),
                vv(ResolvedType.of(Type.Int), 1,2,3),
                vv(ResolvedType.of(Type.Int), 10,20,30)
                ), actual);
        //@formatter:on
    }

    @Test
    public void test_valuevector_int_subscript()
    {
        IExpression expression;
        TupleVector tv;
        ValueVector actual;

        //@formatter:off
        tv = TupleVector.of(Schema.of(
                col("col", Type.Any),
                col("col1", Type.Any),
                Column.of("col2", ResolvedType.valueVector(ResolvedType.of(Type.Int)))
                ),
                asList(
                    vv(ResolvedType.of(Type.Any), 1,null,3),
                    vv(ResolvedType.of(Type.Any),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value"), MapUtils.entry("key1", 123)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value10"), MapUtils.entry("key1", 1230)),
                            MapUtils.ofEntries(true, MapUtils.entry("key", "value100"), MapUtils.entry("key1", 12300))), 
                    vv(ResolvedType.valueVector(ResolvedType.of(Type.Int)),
                            vv(ResolvedType.of(Type.Int), 4,5,6),
                            null,
                            vv(ResolvedType.of(Type.Int), 17,18,19)
                            )
                    ));
        //@formatter:on

        // Subscript with int values on index 2
        expression = new SubscriptExpression(ce("col2", ResolvedType.valueVector(ResolvedType.of(Type.Any))), intLit(2));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 6, null, 19), actual);

        // Subscript with Object values
        expression = new SubscriptExpression(ce("col2", ResolvedType.valueVector(ResolvedType.of(Type.Any))), ce("col", ResolvedType.of(Type.Any)));
        actual = expression.eval(tv, context);

        assertEquals(ResolvedType.of(Type.Any), expression.getType());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(nvv(ResolvedType.of(Type.Int), 5, null, null), actual);

        // Test subscript with compile time value that is not supported
        try
        {
            expression = new SubscriptExpression(ce("col2", ResolvedType.valueVector(ResolvedType.of(Type.Any))), LiteralBooleanExpression.FALSE);
            actual = expression.eval(tv, context);
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a ValueVector with Boolean"));
        }

        try
        {
            expression = new SubscriptExpression(ce("col2", ResolvedType.valueVector(ResolvedType.of(Type.Any))), LiteralBooleanExpression.FALSE);
            expression.getType();
            fail("Should fail with unsupported subscript");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot subscript a ValueVector with Boolean"));
        }
    }
}
