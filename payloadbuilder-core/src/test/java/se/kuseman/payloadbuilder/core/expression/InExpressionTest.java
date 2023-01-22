package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link InExpression} */
public class InExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        InExpression e;
        ValueVector actual;
        // TupleVector tv;

        // Arg null
        e = new InExpression(intLit(10), asList(new LiteralNullExpression(ResolvedType.of(Type.Int))), false);
        actual = e.eval(context);
        assertEquals(ResolvedType.of(Type.Boolean), e.getType());
        assertVectorsEquals(vv(Type.Boolean, (Object) null), actual);

        // Value null
        e = new InExpression(new LiteralNullExpression(ResolvedType.of(Type.Int)), asList(intLit(10)), false);
        actual = e.eval(context);
        assertEquals(ResolvedType.of(Type.Boolean), e.getType());
        assertVectorsEquals(vv(Type.Boolean, (Object) null), actual);

        e = new InExpression(intLit(10), asList(intLit(20), intLit(10)), false);
        actual = e.eval(context);
        assertEquals(ResolvedType.of(Type.Boolean), e.getType());
        assertVectorsEquals(vv(Type.Boolean, true), actual);

        // Not
        e = new InExpression(intLit(10), asList(intLit(20), intLit(10)), true);
        actual = e.eval(context);
        assertEquals(ResolvedType.of(Type.Boolean), e.getType());
        assertVectorsEquals(vv(Type.Boolean, false), actual);
    }
}
