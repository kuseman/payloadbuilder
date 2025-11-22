package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralObjectExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link ObjectFunction} and {@link OperatorObjectFunction} */
class ObjectFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo scalar = SystemCatalog.get()
            .getScalarFunction("object");
    private final OperatorFunctionInfo operator = SystemCatalog.get()
            .getOperatorFunction("object");
    private final IExpression col1 = ce("col1");
    private final IExpression col2 = ce("col2");

    @Test
    void test_fold()
    {
        assertEquals(new LiteralObjectExpression(ObjectVector.EMPTY), scalar.fold(context, asList()));

        ObjectVector expected = ObjectVector
                .wrap(TupleVector.of(Schema.of(Column.of("key", ResolvedType.of(Type.Any)), Column.of("key2", ResolvedType.of(Type.Any))), asList(vv(Type.Any, "value"), vv(Type.Any, 123))));

        assertEquals(new LiteralObjectExpression(expected),
                scalar.fold(context, asList(new LiteralStringExpression("key"), new LiteralStringExpression("value"), new LiteralStringExpression("key2"), intLit(123))));
    }

    @Test
    void test_wrong_number_of_args()
    {
        TupleVector input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        try
        {
            scalar.evalScalar(context, input, "", asList(col1));
            fail("Should fail cause of non even arg count");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Function object requires an even number of arguments"), e.getMessage());
        }

        try
        {
            scalar.getType(asList(col1));
            fail("Should fail cause of non even arg count");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Function object requires an even number of arguments"), e.getMessage());
        }
    }

    @Test
    void test_wrong_type_of_key_args()
    {
        TupleVector input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        try
        {
            scalar.evalScalar(context, input, "", asList(intLit(10), col1));
            fail("Should fail cause of non even arg count");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Function object requires literal string keys"), e.getMessage());
        }

        try
        {
            scalar.getType(asList(intLit(10), col1));
            fail("Should fail cause of non even arg count");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage()
                    .contains("Function object requires literal string keys"), e.getMessage());
        }
    }

    @Test
    void test_aggregate()
    {
        TupleVector input;
        TupleVector tupleVector;
        ValueVector actual;
        Schema schema;
        IAggregator aggregator;

        // No input rows
        input = TupleVector.EMPTY;

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(new LiteralStringExpression("col1"), col1));
        aggregator.appendGroup(input, vv(Type.Int), vv(ResolvedType.array(Type.Int)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(Column.of("col1", Column.Type.Any));
        assertEquals(ResolvedType.object(schema), scalar.getAggregateType(asList(new LiteralStringExpression("col1"), col1)));
        assertVectorsEquals(vv(ResolvedType.object(schema)), actual);

        input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(new LiteralStringExpression("col1"), col1));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3)));
        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector)), actual);

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(new LiteralStringExpression("col1"), col1, new LiteralStringExpression("col2"), col2));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(Column.of("col1", Column.Type.Any), Column.of("col2", Column.Type.Any));
        tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector)), actual);

        // Multi vector
        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(new LiteralStringExpression("col1"), col1, new LiteralStringExpression("col2"), col2));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);

        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(Column.of("col1", Column.Type.Any), Column.of("col2", Column.Type.Any));
        tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector)), actual);
    }

    @Test
    void test_scalar()
    {
        TupleVector input;
        ValueVector actual;
        TupleVector tupleVector;
        Schema schema;

        // No input rows
        // input = TupleVector.EMPTY;
        // actual = scalar.evalScalar(context, input, "", asList(new LiteralStringExpression("col1"), col1));
        schema = Schema.of(Column.of("col1", Column.Type.Any));
        // assertEquals(ResolvedType.object(schema), scalar.getType(asList(col1)));
        // assertVectorsEquals(vv(ResolvedType.table(schema), (TupleVector) null), actual);

        input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        actual = scalar.evalScalar(context, input, "", asList(new LiteralStringExpression("col1"), col1));
        assertEquals(ResolvedType.object(schema), scalar.getType(asList(new LiteralStringExpression("col1"), col1)));
        tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3)));
        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector, 0), ObjectVector.wrap(tupleVector, 1), ObjectVector.wrap(tupleVector, 2)), actual);

        actual = scalar.evalScalar(context, input, "", asList(new LiteralStringExpression("col1"), col1, new LiteralStringExpression("col2"), col2));
        schema = Schema.of(Column.of("col1", Column.Type.Any), Column.of("col2", Column.Type.Any));
        assertEquals(ResolvedType.object(schema), scalar.getType(asList(new LiteralStringExpression("col1"), col1, new LiteralStringExpression("col2"), col2)));
        tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector, 0), ObjectVector.wrap(tupleVector, 1), ObjectVector.wrap(tupleVector, 2)), actual);
    }

    @Test
    void test_operator()
    {
        TupleVector input;
        ValueVector actual;

        Schema schema = Schema.of(Column.of("col1", Column.Type.Any), Column.of("col2", Column.Type.Any));

        // No input rows
        input = TupleVector.EMPTY;
        actual = operator.eval(context, "", input);
        assertVectorsEquals(vv(ResolvedType.object(Schema.EMPTY), (ObjectVector) null), actual);

        input = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        assertEquals(operator.getType(input.getSchema()), ResolvedType.object(schema));

        actual = operator.eval(context, "", input);

        TupleVector tupleVector = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));

        assertVectorsEquals(vv(ResolvedType.object(schema), ObjectVector.wrap(tupleVector, 0)), actual);
    }
}
