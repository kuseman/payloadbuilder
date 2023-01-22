package se.kuseman.payloadbuilder.core.expression;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link LogicalNotExpression} */
public class LogicalNotExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        LogicalNotExpression e;
        ValueVector actual;

        e = new LogicalNotExpression(new LiteralNullExpression(ResolvedType.of(Type.Boolean)));
        actual = e.eval(TupleVector.CONSTANT, context);
        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.Boolean, (Boolean) null), actual);

        e = new LogicalNotExpression(new LiteralBooleanExpression(false));
        actual = e.eval(TupleVector.CONSTANT, context);
        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.Boolean, true), actual);

        e = new LogicalNotExpression(new LiteralBooleanExpression(true));
        actual = e.eval(TupleVector.CONSTANT, context);
        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.Boolean, false), actual);
    }
}
