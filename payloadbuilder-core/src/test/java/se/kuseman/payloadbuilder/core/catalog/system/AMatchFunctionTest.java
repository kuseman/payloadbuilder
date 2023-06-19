package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
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

/** Test of {@link AMatchFunction} */
public class AMatchFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo all = SystemCatalog.get()
            .getScalarFunction("all");
    private final ScalarFunctionInfo any = SystemCatalog.get()
            .getScalarFunction("any");
    private final ScalarFunctionInfo none = SystemCatalog.get()
            .getScalarFunction("none");

    @Test
    public void test_iterable_input()
    {
        assertIterableMatchFunction(all, new Object[] { false, true, false }, new Object[] { false, false, true, true });
        assertIterableMatchFunction(any, new Object[] { true, false, true }, new Object[] { false, true, false, true });
        assertIterableMatchFunction(none, new Object[] { false, true, false }, new Object[] { true, false, true, false });
    }

    @Test
    public void test_non_iterable_input()
    {
        assertNonIterableMatchFunction(all, new Object[] { false, true, true, false }, new Object[] { false, false, true, true });
        assertNonIterableMatchFunction(any, new Object[] { false, true, true, false }, new Object[] { false, false, true, true });
        assertNonIterableMatchFunction(none, new Object[] { true, false, false, true }, new Object[] { true, true, false, false });
    }

    private void assertIterableMatchFunction(ScalarFunctionInfo function, Object[] result1, Object[] result2)
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // ValueVector
        //@formatter:off
        tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.valueVector(ResolvedType.of(Type.Int)))), asList(
                vv(ResolvedType.valueVector(ResolvedType.of(Type.Int)),
                        vv(ResolvedType.of(Type.Int), 3, 4, 5, 6), 
                        vv(ResolvedType.of(Type.Int)), 
                        vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5, 6))));
        //@formatter:on
        input = ce("col", ResolvedType.valueVector(ResolvedType.of(Type.Int)), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int)), intLit(3)), new int[] { 0 });
        actual = function.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Boolean), all.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.of(Type.Boolean), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), result1), actual);

        // TupleVector
        Schema innerSchema = Schema.of(col("key1", Type.Int), col("key2", Type.String));
        //@formatter:off
        tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.tupleVector(innerSchema))), asList(
                vv(ResolvedType.tupleVector(innerSchema), 
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 1, 2, 3), vv(ResolvedType.of(Type.String), "one", "two", "three"))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5, 6), vv(ResolvedType.of(Type.String), "four", "five", "six"))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int)), vv(ResolvedType.of(Type.String)))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 7, 8, 9), vv(ResolvedType.of(Type.String), "seven", "eight", "nine"))))
                ));
        //@formatter:on

        input = ce("col", ResolvedType.tupleVector(innerSchema), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int), 0), intLit(4)), new int[] { 0 });
        actual = function.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Boolean), all.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Boolean), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), result2), actual);
    }

    private void assertNonIterableMatchFunction(ScalarFunctionInfo function, Object[] result1, Object[] result2)
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // Int output
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(ResolvedType.of(Type.Int), 10, 20, 30, null)));
        input = ce("col", ResolvedType.of(Type.Int), 0);
        lambdaExpression = new LambdaExpression(asList("x"), gt(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10)), new int[] { 0 });
        actual = function.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Boolean), all.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Boolean), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), result1), actual);

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
        actual = function.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Boolean), all.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Boolean), actual.type());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), result2), 
                actual);
        //@formatter:on
    }
}
