package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.nvv;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link FlatMapFunction} */
public class FlatMapFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo flatMapFunction = SystemCatalog.get()
            .getScalarFunction("flatmap");
    private final ScalarFunctionInfo listOfFunction = SystemCatalog.get()
            .getScalarFunction("listof");
    private final ScalarFunctionInfo toListFunction = SystemCatalog.get()
            .getScalarFunction("toList");

    @Test
    public void test_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // List
        // col is Object of Lists
        // col: [ [1,2,3], [4,5,6] ]
        // result: [ [10,20,30], [40,50,60] ]
        // col.map(x -> x + 10) ==> boom [1,2,3] + 10 invalid
        // col.map(x -> vector(x).map(y -> y + 10))
        //

        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), asList(1, 2, 3), asList(4, null, 6), asList(10))));
        input = new FunctionCallExpression("", toListFunction, null, asList(ce("col", ResolvedType.of(Type.Any), 0)));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", listOfFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10))), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), flatMapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), actual.type());
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 1, 10, 2, 10, 3, 10), (ValueVector) actual.getValue(0));
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 4, 10, null, 10, 6, 10), (ValueVector) actual.getValue(1));
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 10, 10), (ValueVector) actual.getValue(2));

        // TupleVector
        Schema innerSchema = Schema.of(col("key1", Type.Int), col("key2", Type.String));
        //@formatter:off
            tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.tupleVector(innerSchema))), asList(
                    vv(ResolvedType.tupleVector(innerSchema), 
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 1, 2, 3), vv(ResolvedType.of(Type.String), "one", "two", "three"))),
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5, 6), vv(ResolvedType.of(Type.String), "four", "five", "six"))),
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 7, 8, 9), vv(ResolvedType.of(Type.String), "seven", "eight", "nine"))))
                    ));
        //@formatter:on

        input = ce("col", ResolvedType.tupleVector(innerSchema), 0);
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", listOfFunction, null, asList(lce("x", 0, ResolvedType.of(Type.String), 1), intLit(10))), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), flatMapFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.valueVector(ResolvedType.of(Type.Any)),
                nvv(ResolvedType.of(Type.Any), "one", 10, "two", 10, "three", 10),
                nvv(ResolvedType.of(Type.Any), "four", 10, "five", 10, "six", 10),
                nvv(ResolvedType.of(Type.Any), "seven", 10, "eight", 10, "nine", 10)
                ), actual);
        //@formatter:on
    }

    @Test
    public void test_non_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // Int output, this behaves the same as map since the input is flat already
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, null)));
        input = ce("col", ResolvedType.of(Type.Int), 0);
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", listOfFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10))), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        // input: [1,2,3,null]
        // col.flatMap(x -> listOf(x, 10))
        // output: [ [1,10], [2, 10], [3, 10], [null, 10] ]

        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), flatMapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Any)), actual.type());
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 1, 10), (ValueVector) actual.getValue(0));
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 2, 10), (ValueVector) actual.getValue(1));
        assertVectorsEquals(nvv(ResolvedType.of(Type.Any), 3, 10), (ValueVector) actual.getValue(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 10), (ValueVector) actual.getValue(3));
    }
}
