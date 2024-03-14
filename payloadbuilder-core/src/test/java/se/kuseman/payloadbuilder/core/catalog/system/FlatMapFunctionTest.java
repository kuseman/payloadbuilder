package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link FlatMapFunction} */
public class FlatMapFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo flatMapFunction = SystemCatalog.get()
            .getScalarFunction("flatmap");
    private final ScalarFunctionInfo arrayFunction = SystemCatalog.get()
            .getScalarFunction("array");
    private final ScalarFunctionInfo toArrayFunction = SystemCatalog.get()
            .getScalarFunction("toarray");

    @Test
    public void test_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // Any
        // col is Object of Lists
        // col: [ [1,2,3], [4,null,6], [10] ]
        // col.flatMap(x -> toarray(x, 10))
        // result: [ [1,10,2,10,3,10], [4,10,null,10,6,10], [10,10] ]

        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), asList(1, 2, 3), asList(4, null, 6), null, asList(10))));
        input = new FunctionCallExpression("", toArrayFunction, null, asList(ce("col", 0, ResolvedType.of(Type.Any))));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", arrayFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10))), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), flatMapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 1, 10, 2, 10, 3, 10), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 4, 10, null, 10, 6, 10), actual.getArray(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), null, 10), actual.getArray(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), 10, 10), actual.getArray(3));

        // TupleVector
        Schema innerSchema = Schema.of(col("key1", Type.Int), col("key2", Type.String));
        //@formatter:off
            tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.table(innerSchema))), asList(
                    vv(ResolvedType.table(innerSchema),
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 1, 2, 3), vv(ResolvedType.of(Type.String), "one", "two", "three"))),
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5, 6), vv(ResolvedType.of(Type.String), "four", "five", "six"))),
                            TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 7, 8, 9), vv(ResolvedType.of(Type.String), "seven", "eight", "nine"))))
                    ));
        //@formatter:on

        input = ce("col", 0, ResolvedType.table(innerSchema));
        lambdaExpression = new LambdaExpression(asList("x"),
                new FunctionCallExpression("", arrayFunction, null, asList(DereferenceExpression.create(lce("x", 0, ResolvedType.object(innerSchema)), QualifiedName.of("key2"), null), intLit(10))),
                new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), flatMapFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.of(Type.Any)),
                vv(ResolvedType.of(Type.Any), "one", 10, "two", 10, "three", 10),
                vv(ResolvedType.of(Type.Any), "four", 10, "five", 10, "six", 10),
                vv(ResolvedType.of(Type.Any), "seven", 10, "eight", 10, "nine", 10)
                ), actual);
        //@formatter:on

        // Any lambda type with mixed single value and arrays
        //@formatter:off
        tv = TupleVector.of(Schema.of(Column.of("col", ResolvedType.array(Type.Any))),
                asList(vv(ResolvedType.array(Type.Any),
                    vv(ResolvedType.of(Type.Any)),
                    null,
                    vv(ResolvedType.of(Type.Any), 1, vv(ResolvedType.of(Type.Any), 2, 3, null, 4))
                )));
        //@formatter:on
        input = ce("col", 0, ResolvedType.array(Type.Any));
        lambdaExpression = new LambdaExpression(asList("x"), lce("x", 0, ResolvedType.of(Type.Any)), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(Type.Any), flatMapFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.array(Type.Any),
                vv(ResolvedType.of(Type.Any)),
                null,
                vv(ResolvedType.of(Type.Any), 1, 2, 3, null, 4)
                ), actual);
        //@formatter:on

        // Mix of different values as Any
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), new Object[] { 1, 2, asList(4, 5, 6), null }, "hello", asList(10.11F, 20.22F))));
        input = ce("col", 0, ResolvedType.of(Type.Any));
        lambdaExpression = new LambdaExpression(asList("x"), lce("x", 0, ResolvedType.of(Type.Any)), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Any), flatMapFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                vv(ResolvedType.of(Type.Any), 1, 2, 4, 5, 6, null),
                "hello",
                vv(ResolvedType.of(Type.Any), 10.11F, 20.22F)
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
        input = ce("col", 0, ResolvedType.of(Type.Int));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", arrayFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10))), new int[] { 0 });
        actual = flatMapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        // input: [1,2,3,null]
        // col.flatMap(x -> arrayOf(x, 10))
        // output: [ [1,10], [2, 10], [3, 10], [null, 10] ]

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), flatMapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 1, 10), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 2, 10), actual.getArray(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 3, 10), actual.getArray(2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), null, 10), actual.getArray(3));
    }
}
