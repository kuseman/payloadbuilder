package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralFloatExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link CoalesceFunction} */
class CoalesceFunctionTest extends APhysicalPlanTest
{
    ScalarFunctionInfo f = SystemCatalog.get()
            .getScalarFunction("coalesce");

    @Test
    void test_type()
    {
        assertEquals(ResolvedType.of(Type.Int), f.getType(asList(intLit(1))));
        assertEquals(ResolvedType.of(Type.Float), f.getType(asList(intLit(1), new LiteralFloatExpression(10f))));
    }

    @Test
    void test()
    {
        ValueVector actual;

        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));
        IExpression col2 = ce("col2", ResolvedType.of(Type.Float));
        IExpression col6 = ce("col6", ResolvedType.of(Type.Long));

        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.Int)),
                Column.of("col2", ResolvedType.of(Type.Float)),
                Column.of("col3", ResolvedType.of(Type.String)),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.Long))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5),
                vv(Type.Float, 1F, null, 4F, 5F),
                vv(Type.String, "one", "two", null, "four"),
                vv(Type.Boolean, true, false, true, null),
                vv(Type.Long, 100L, 200L, 300L, 400L),
                vv(Type.Long, null, null, null, null)
                ));
        //@formatter:on

        assertEquals(ResolvedType.of(Type.Float), f.getType(asList(col1, col2)));
        actual = f.evalScalar(context, input, "", asList(col1, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, 2F, 4F, 5F), actual);

        assertEquals(ResolvedType.of(Type.Float), f.getType(asList(col6, col2)));
        actual = f.evalScalar(context, input, "", asList(col6, col2));
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), 1F, null, 4F, 5F), actual);
    }
}
