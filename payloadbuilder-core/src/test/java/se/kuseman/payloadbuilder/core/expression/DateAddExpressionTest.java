package se.kuseman.payloadbuilder.core.expression;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link DateAddExpression} */
public class DateAddExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test() throws Exception
    {
        DateAddExpression e;
        ValueVector actual;

        // Nulls
        e = new DateAddExpression(IDatePartExpression.Part.HOUR, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralNullExpression(ResolvedType.of(Type.String)));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), new Object[] { null }), actual);

        e = new DateAddExpression(IDatePartExpression.Part.HOUR, new LiteralNullExpression(ResolvedType.of(Type.Int)), new LiteralStringExpression("hello"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), new Object[] { null }), actual);

        e = new DateAddExpression(IDatePartExpression.Part.HOUR, new LiteralIntegerExpression(1), new LiteralStringExpression("2010-10-10"));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), EpochDateTime.from("2010-10-10T01:00:00")), actual);

        e = new DateAddExpression(IDatePartExpression.Part.HOUR, new LiteralIntegerExpression(1), new CastExpression(new LiteralStringExpression("2010-10-10"), ResolvedType.of(Type.DateTimeOffset)));
        actual = e.eval(context);
        assertVectorsEquals(vv(ResolvedType.of(Type.DateTimeOffset), EpochDateTimeOffset.from("2010-10-10T01:00:00")), actual);
    }
}
