package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Test of {@link MapFunction} */
class MapFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo mapFunction = SystemCatalog.get()
            .getScalarFunction("map");
    private final ScalarFunctionInfo concatFunction = SystemCatalog.get()
            .getScalarFunction("concat");
    private final ScalarFunctionInfo toArrayFunction = SystemCatalog.get()
            .getScalarFunction("toarray");

    @Test
    void test_nested_mapping()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression1;
        IExpression lambdaExpression2;

        // col is ValueVector of ints
        // col: [ [1,2,3], [4,5,6] ]
        // result: [ [10,20,30], [40,50,60] ]
        // col.map(x -> x.map(y -> y + 10))
        //

        tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.array(ResolvedType.of(Type.Int)))),
                asList(vv(ResolvedType.array(ResolvedType.of(Type.Int)), vv(ResolvedType.of(Type.Int), 1, 2, 3), vv(ResolvedType.of(Type.Int), 4, 5))));
        input = ce("col", 0, ResolvedType.array(ResolvedType.of(Type.Int)));
        lambdaExpression1 = new LambdaExpression(asList("y"), add(lce("y", 1, ResolvedType.of(Type.Int)), intLit(10)), new int[] { 1 });
        lambdaExpression2 = new LambdaExpression(asList("x"), new FunctionCallExpression("", mapFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Int)), lambdaExpression1)), new int[] { 0 });

        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression2));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), mapFunction.getType(asList(input, lambdaExpression2)));
        assertEquals(2, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 14, 15), actual.getArray(1));
    }

    @Test
    void test_mapping_populated_column()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        /*
         * d: TupleVector d.map(x -> x.col)
         * 
         * 
         * 
         */

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
        lambdaExpression = new LambdaExpression(asList("x"), DereferenceExpression.create(lce("x", 0, ResolvedType.object(innerSchema)), QualifiedName.of("key2"), null), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.array(ResolvedType.of(Type.String)),
                vv(ResolvedType.of(Type.String), "one", "two", "three"),
                vv(ResolvedType.of(Type.String), "four", "five", "six"),
                vv(ResolvedType.of(Type.String), "seven", "eight", "nine")
                ) , actual);
        //@formatter:on
    }

    @Test
    void test_iterable_input()
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

        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), asList(1, 2, 3), asList(4, null, 6))));
        input = new FunctionCallExpression("", toArrayFunction, null, asList(ce("col", 0, ResolvedType.of(Type.Any))));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(2, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 14, null, 16), actual.getArray(1));

        // Collection (set)
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), CollectionUtils.asOrderedSet(1, 2, 3, 4, 5, 6, null))));
        input = new FunctionCallExpression("", toArrayFunction, null, asList(ce("col", 0, ResolvedType.of(Type.Any))));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(1, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13, 14, 15, 16, null), actual.getArray(0));

        // Array
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), new int[] { 1, 2, 3, 4, 5, 6 })));
        input = new FunctionCallExpression("", toArrayFunction, null, asList(ce("col", 0, ResolvedType.of(Type.Any))));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(1, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13, 14, 15, 16), actual.getArray(0));

        // Mix of different values wrapped in Array
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), new int[] { 1, 2, 3, 4, 5, 6 }, "hello", asList(10.11F, 20.22F))));
        input = new FunctionCallExpression("", toArrayFunction, null, asList(ce("col", 0, ResolvedType.of(Type.Any))));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", concatFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10))), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "110", "210", "310", "410", "510", "610"), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "hello10"), actual.getArray(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "10.1110", "20.2210"), actual.getArray(2));

        // Mix of different values as Any
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), new int[] { 1, 2, 3, 4, 5, 6 }, "hello", asList(10.11F, 20.22F))));
        input = ce("col", 0, ResolvedType.of(Type.Any));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", concatFunction, null, asList(lce("x", 0, ResolvedType.of(Type.Any)), intLit(10))), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Any), mapFunction.getType(asList(input, lambdaExpression)));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.of(Type.Any),
                vv(ResolvedType.of(Type.String), "110", "210", "310", "410", "510", "610"),
                "hello10",
                vv(ResolvedType.of(Type.String), "10.1110", "20.2210")
                ), actual);
        //@formatter:on

        // ValueVector
        tv = TupleVector.of(Schema.of(Column.of("col", ResolvedType.array(ResolvedType.of(Type.Int)))),
                asList(vv(ResolvedType.array(ResolvedType.of(Type.Int)), vv(ResolvedType.of(Type.Int), 1, 2, 3, 4, 5, 6), null)));
        input = ce("col", 0, ResolvedType.array(ResolvedType.of(Type.Int)));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(2, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13, 14, 15, 16), actual.getArray(0));
        assertNull(actual.getArray(1));

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
                new FunctionCallExpression("", concatFunction, null, asList(DereferenceExpression.create(lce("x", 0, ResolvedType.object(innerSchema)), QualifiedName.of("key2"), null), intLit(10))),
                new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "one10", "two10", "three10"), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "four10", "five10", "six10"), actual.getArray(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "seven10", "eight10", "nine10"), actual.getArray(2));

        // TupleVector with access to input column

        // col.map(x -> concat(x, key))

        //@formatter:off
        tv = TupleVector.of(Schema.of(new Column("col", ResolvedType.table(innerSchema)), col("key", Type.Int)), asList(
                vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 1, 2, 3), vv(ResolvedType.of(Type.String), "one", "two", "three"))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 4, 5, 6), vv(ResolvedType.of(Type.String), "four", "five", "six"))),
                        TupleVector.of(innerSchema, asList(vv(ResolvedType.of(Type.Int), 7, 8, 9), vv(ResolvedType.of(Type.String), "seven", "eight", "nine")))),
                vv(ResolvedType.of(Type.Int), 10, 20, 30)
                ));
        //@formatter:on

        lambdaExpression = new LambdaExpression(asList("x"),
                new FunctionCallExpression("", concatFunction, null, asList(DereferenceExpression.create(lce("x", 0, ResolvedType.object(innerSchema)), QualifiedName.of("key2"), null), ce("key"))),
                new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.array(ResolvedType.of(Type.String)), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "one10", "two10", "three10"), actual.getArray(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "four20", "five20", "six20"), actual.getArray(1));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "seven30", "eight30", "nine30"), actual.getArray(2));
    }

    @Test
    void test_non_iterable_input()
    {
        ValueVector actual;
        TupleVector tv;
        IExpression input;
        IExpression lambdaExpression;

        // Int output
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(ResolvedType.of(Type.Int), 1, 2, 3, null)));
        input = ce("col", 0, ResolvedType.of(Type.Int));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Int)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Int), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 11, 12, 13, null), actual);

        // Long output
        tv = TupleVector.of(Schema.of(col("col", Type.Long)), asList(vv(ResolvedType.of(Type.Long), 1L, 5L, null, 6L)));
        input = ce("col", 0, ResolvedType.of(Type.Long));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Long)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Long), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Long), 11L, 15L, null, 16L), actual);

        // Float output
        tv = TupleVector.of(Schema.of(col("col", Type.Float)), asList(vv(ResolvedType.of(Type.Float), 1.1F, null, 2.2F, 3.3F)));
        input = ce("col", 0, ResolvedType.of(Type.Float));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Float)), intLit(10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Float), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 11.1F, null, 12.2F, 13.3F), actual);

        // Double output
        tv = TupleVector.of(Schema.of(col("col", Type.Double)), asList(vv(ResolvedType.of(Type.Double), -1.1D, -2.2D, null, -3.3D)));
        input = ce("col", 0, ResolvedType.of(Type.Double));
        lambdaExpression = new LambdaExpression(asList("x"), add(lce("x", 0, ResolvedType.of(Type.Double)), intLit(-10)), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Double), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(4, actual.size());
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Double), -11.1D, -12.2D, null, -13.3D), actual);

        // Boolean output
        tv = TupleVector.of(Schema.of(col("col", Type.Boolean)), asList(vv(ResolvedType.of(Type.Boolean), true, false, null)));
        input = ce("col", 0, ResolvedType.of(Type.Boolean));
        lambdaExpression = new LambdaExpression(asList("x"), and(lce("x", 0, ResolvedType.of(Type.Boolean)), LiteralBooleanExpression.TRUE), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Boolean), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.of(Type.Boolean), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, false, null), actual);

        // String output
        tv = TupleVector.of(Schema.of(col("col", Type.String)), asList(vv(ResolvedType.of(Type.String), "hello", "world", null)));
        input = ce("col", 0, ResolvedType.of(Type.String));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("concat"), null, asList(lce("x", 0, ResolvedType.of(Type.String)), new LiteralStringExpression("hello"))), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.String), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.of(Type.String), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "hellohello", "worldhello", "hello"), actual);

        // Object output
        tv = TupleVector.of(Schema.of(col("col", Type.Any)), asList(vv(ResolvedType.of(Type.Any), "hello", "world", null)));
        input = ce("col", 0, ResolvedType.of(Type.Any));
        lambdaExpression = new LambdaExpression(asList("x"), new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("concat"), null, asList(lce("x", 0, ResolvedType.of(Type.Any)), new LiteralStringExpression("hello"))), new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Any), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertVectorsEquals(vv(ResolvedType.of(Type.Any), "hellohello", "worldhello", "hello"), actual);

        // Map
        tv = TupleVector.of(Schema.of(col("col", Type.Any)),
                asList(vv(ResolvedType.of(Type.Any), MapUtils.ofEntries(true, MapUtils.entry("key", "value"), MapUtils.entry("key2", "value2")),
                        MapUtils.ofEntries(true, MapUtils.entry("key", "value2"), MapUtils.entry("key2", "value3")),
                        MapUtils.ofEntries(true, MapUtils.entry("key3", "value2"), MapUtils.entry("key2", "value3")))));
        input = ce("col", 0, ResolvedType.of(Type.Any));
        lambdaExpression = new LambdaExpression(asList("x"),
                new FunctionCallExpression("", concatFunction, null, asList(new DereferenceExpression(lce("x", 0, ResolvedType.of(Type.Any)), "key", -1, ResolvedType.of(Type.Any)), intLit(10))),
                new int[] { 0 });
        actual = mapFunction.evalScalar(context, tv, "", asList(input, lambdaExpression));

        assertEquals(ResolvedType.of(Type.Any), mapFunction.getType(asList(input, lambdaExpression)));
        assertEquals(3, actual.size());
        assertEquals(ResolvedType.of(Type.Any), actual.type());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), "value10", "value210", "10"), actual);
    }
}
