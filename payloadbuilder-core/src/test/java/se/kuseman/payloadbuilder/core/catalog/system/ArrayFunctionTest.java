package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralArrayExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link ArrayFunction} */
public class ArrayFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo scalar = SystemCatalog.get()
            .getScalarFunction("array");
    private final IExpression col1 = ce("col1");
    private final IExpression col2 = ce("col2");

    @Test
    public void test_fold()
    {
        IExpression fold = scalar.fold(context, asList());
        assertEquals(new LiteralArrayExpression(vv(Type.Any)), fold);

        fold = scalar.fold(context, asList(intLit(10), intLit(20), intLit(-20)));
        assertEquals(new LiteralArrayExpression(vv(Type.Any, 10, 20, -20)), fold);
    }

    @Test
    public void test_scalar()
    {
        ValueVector actual = scalar.evalScalar(context, TupleVector.CONSTANT, "", asList(intLit(10), intLit(20)));

        assertEquals(ResolvedType.array(Column.Type.Int), scalar.getType(asList(intLit(10), intLit(20))));
        assertVectorsEquals(vv(ResolvedType.array(Column.Type.Int), vv(Column.Type.Int, 10, 20)), actual);
    }

    @Test
    public void test_aggregate()
    {
        TupleVector input;
        ValueVector actual;
        IAggregator aggregator;

        // No input rows
        input = TupleVector.EMPTY;

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int), vv(ResolvedType.array(Type.Int)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        assertEquals(ResolvedType.array(Column.Type.Any), scalar.getAggregateType(asList(col1)));
        assertVectorsEquals(vv(ResolvedType.array(Type.Any)), actual);

        /*
         * @formatter:off
         * 
         * TupleVector1:
         *   col1: 1,2,3
         *   col2: 4,5,6
         * 
         * sum(col1) => 1+2+3=6
         * count(col1) => 3
         * array(col1) => [1,2,3]
         * array(col1, col2) => [1,4,2,5,3,6]
         * 
         * 
         * @formatter:on
         */

        input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(ResolvedType.of(Type.Any), 1, 2, 3)), actual);

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1, col2));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(ResolvedType.of(Type.Any), 1, 4, 2, 5, 3, 6)), actual);

        // Mix both aggregate and scalar array call
        // array(array(col1, col2))

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(new FunctionCallExpression("", scalar, null, asList(col1, col2))));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(Type.Any, vv(Type.Any, 1, 4), vv(Type.Any, 2, 5), vv(Type.Any, 3, 6))), actual);
    }
}
