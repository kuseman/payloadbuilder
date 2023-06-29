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
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link RegexpLikeFunction} */
public class RegexpLikeFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo f = SystemCatalog.get()
            .getScalarFunction("regexp_like");

    @Test
    public void test()
    {
        assertEquals(ResolvedType.of(Type.Boolean), f.getType(asList()));

        IExpression col3 = ce("col3", ResolvedType.of(Type.String));
        IExpression col6 = ce("col6", ResolvedType.of(Type.String));

      //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.Int)),
                Column.of("col2", ResolvedType.of(Type.Float)),
                Column.of("col3", ResolvedType.of(Type.String)),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.String))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5),
                vv(Type.Float, 1F, null, 4F, 5F),
                vv(Type.String, "one", "two", null, "four"),
                vv(Type.Boolean, true, false, true, null),
                vv(Type.Long, 100L, 200L, 300L, 400L),
                vv(Type.String, "a", "b", null, "c")
                ));
        //@formatter:on

        ValueVector actual;

        actual = f.evalScalar(context, input, "", asList(col3, new LiteralStringExpression("o")));
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, true, null, true), actual);

        actual = f.evalScalar(context, input, "", asList(col3, col6));
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, null, false), actual);

        actual = f.evalScalar(context, input, "", asList(new LiteralStringExpression("a string"), col6));
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), true, false, null, false), actual);

        actual = f.evalScalar(context, input, "", asList(new LiteralStringExpression("a string"), new LiteralStringExpression("o")));
        assertVectorsEquals(vv(ResolvedType.of(Type.Boolean), false, false, false, false), actual);
    }
}
