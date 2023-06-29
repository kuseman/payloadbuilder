package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralFloatExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link LeastGreatestFunction} */
public class LeastGreatestFunctionTest extends APhysicalPlanTest
{
    ScalarFunctionInfo least = SystemCatalog.get()
            .getScalarFunction("least");
    ScalarFunctionInfo greatest = SystemCatalog.get()
            .getScalarFunction("greatest");

    @Test
    public void test_type()
    {
        assertEquals(ResolvedType.of(Type.Int), least.getType(asList(intLit(1))));
        assertEquals(ResolvedType.of(Type.Float), least.getType(asList(intLit(1), new LiteralFloatExpression(10f))));
        assertEquals(Arity.AT_LEAST_TWO, least.arity());
    }

    @Test
    public void test()
    {
        ValueVector actual;

        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));
        IExpression col2 = ce("col2", ResolvedType.of(Type.Float));
        IExpression col3 = ce("col3", ResolvedType.of(Type.String));
        IExpression col6 = ce("col6", ResolvedType.of(Type.Long));
        IExpression col7 = ce("col7", ResolvedType.of(Type.String));

        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.Int)),
                Column.of("col2", ResolvedType.of(Type.Float)),
                Column.of("col3", ResolvedType.of(Type.String)),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.Long)),
                Column.of("col7", ResolvedType.of(Type.String))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5),
                vv(Type.Float, 1F, null, 3F, 5.5F),
                vv(Type.String, "one", "two", null, "four"),
                vv(Type.Boolean, true, false, true, null),
                vv(Type.Long, 100L, 200L, 300L, 400L),
                vv(Type.Long, null, null, null, null),
                vv(Type.String, "five", "six", null, "eight")
                ));
        //@formatter:on

        assertEquals(ResolvedType.of(Type.Float), least.getType(asList(col1, col2)));
        actual = least.evalScalar(context, input, "", asList(col1, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, 2F, 3F, 5F), actual);

        assertEquals(ResolvedType.of(Type.Float), greatest.getType(asList(col1, col2)));
        actual = greatest.evalScalar(context, input, "", asList(col1, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, 2F, 4.0F, 5.5F), actual);

        assertEquals(ResolvedType.of(Type.Float), least.getType(asList(col6, col2)));
        actual = least.evalScalar(context, input, "", asList(col6, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, null, 3F, 5.5F), actual);

        assertEquals(ResolvedType.of(Type.Float), greatest.getType(asList(col6, col2)));
        actual = greatest.evalScalar(context, input, "", asList(col6, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, null, 3F, 5.5F), actual);

        assertEquals(ResolvedType.of(Type.String), least.getType(asList(col3, col7)));
        actual = least.evalScalar(context, input, "", asList(col3, col7));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "five", "six", null, "eight"), actual);

        assertEquals(ResolvedType.of(Type.String), greatest.getType(asList(col3, col7)));
        actual = greatest.evalScalar(context, input, "", asList(col3, col7));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "one", "two", null, "four"), actual);

        assertEquals(ResolvedType.of(Type.Int), least.getType(asList(intLit(10), intLit(20))));
        actual = least.evalScalar(context, input, "", asList(intLit(10), intLit(20)));
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 10, 10, 10, 10), actual);
    }
}
