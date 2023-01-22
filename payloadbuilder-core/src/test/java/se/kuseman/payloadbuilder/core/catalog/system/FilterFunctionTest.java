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
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link FilterFunction} */
public class FilterFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo filterFunction = SystemCatalog.get()
            .getScalarFunction("filter");

    @Test
    public void test_non_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // Int output
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(ResolvedType.of(Type.Int), 10, 20, 30, null)));
        input = ce("col", ResolvedType.of(Type.Int), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10)), new int[] { 0 });
        actual = filterFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Int), filterFunction.getType(asList(input, lambdaExpression)));
        assertEquals(2, actual.size());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertVectorsEquals(nvv(ResolvedType.of(Type.Int), 20, 30), actual);

        // Map's
        //@formatter:off
        tv = TupleVector.of(Schema.of(col("col", Type.Any)),
                asList(vv(ResolvedType.of(Type.Any), 
                        MapUtils.ofEntries(true, MapUtils.entry("key", 10), MapUtils.entry("key2", "value10")),
                        MapUtils.ofEntries(true, MapUtils.entry("key3", 20), MapUtils.entry("key2", "value20")),        // No such key
                        MapUtils.ofEntries(true, MapUtils.entry("key", 30), MapUtils.entry("key2", "value30")),
                        MapUtils.ofEntries(true, MapUtils.entry("key", 40), MapUtils.entry("key2", "value40")))));
        //@formatter:on
        input = ce("col", ResolvedType.of(Type.Any), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(new DereferenceExpression(lce("x", 0, ResolvedType.of(Type.Any)), "key", -1, ResolvedType.of(Type.Any)), intLit(10)), new int[] { 0 });
        actual = filterFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Any), filterFunction.getType(asList(input, lambdaExpression)));
        assertEquals(2, actual.size());
        assertEquals(ResolvedType.of(Type.Any), actual.type());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                MapUtils.ofEntries(true, MapUtils.entry("key", 30), MapUtils.entry("key2", "value30")),
                MapUtils.ofEntries(true, MapUtils.entry("key", 40), MapUtils.entry("key2", "value40"))), 
                actual);
        //@formatter:on
    }

    @Test
    public void test_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // ValueVector
        tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.valueVector(ResolvedType.of(Type.Int)))),
                asList(vv(ResolvedType.valueVector(ResolvedType.of(Type.Int)), vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5, 6))));
        input = ce("col", ResolvedType.valueVector(ResolvedType.of(Type.Int)), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int)), intLit(3)), new int[] { 0 });
        actual = filterFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), filterFunction.getType(asList(input, lambdaExpression)));
        assertEquals(1, actual.size());
        assertEquals(ResolvedType.valueVector(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 5, 6), (ValueVector) actual.getValue(0));

        // TupleVector
        /*
         * col: [ { key1: [1,2,3], key2: [...] }, {}, {} ] x -> x.key1 > 0
         * 
         */
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
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int), 0), intLit(4)), new int[] { 0 });
        actual = filterFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.tupleVector(innerSchema), filterFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(nvv(ResolvedType.tupleVector(innerSchema),
                TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int)), vv(ResolvedType.of(Type.String)))),
                TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 5, 6), vv(ResolvedType.of(Type.String), "five", "six"))),
                TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 7, 8, 9), vv(ResolvedType.of(Type.String), "seven", "eight", "nine")))
                ),
                actual);
        //@formatter:on
    }
}
