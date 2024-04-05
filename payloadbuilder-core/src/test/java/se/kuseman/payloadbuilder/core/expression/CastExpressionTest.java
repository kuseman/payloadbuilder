package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link CastExpression} */
public class CastExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        //@formatter:off
        Schema schema = Schema.of(
                Column.of("int", ResolvedType.of(Type.Int)),
                Column.of("float", ResolvedType.of(Type.Float)),
                Column.of("string", ResolvedType.of(Type.String)),
                Column.of("boolean", ResolvedType.of(Type.Boolean)),
                Column.of("long", ResolvedType.of(Type.Long)),
                Column.of("datetime", ResolvedType.of(Type.DateTime)),
                Column.of("stringInt", ResolvedType.of(Type.String)),
                Column.of("anyValueVector", ResolvedType.array(Type.Any))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5),
                vv(Type.Float, 1F, null, 4F, 5F),
                vv(Type.String, "one", "two", null, "four"),
                vv(Type.Boolean, true, false, true, null),
                vv(Type.Long, 100L, 200L, 300L, 400L),
                vv(Type.DateTime, EpochDateTime.from("2010-10-10"), EpochDateTime.from("2011-10-10"), EpochDateTime.from("2012-10-10"), EpochDateTime.from("2013-10-10")),
                vv(Type.String, "1", "2", null, "4"),
                vv(ResolvedType.array(Type.Any), vv(Type.Int, 1), vv(Type.Long, 2L), null, vv(Type.String, "hello"))
                ));
        //@formatter:on

        IExpression ce;
        ValueVector actual;

        // Cast to int
        ce = new CastExpression(ce("int"), ResolvedType.of(Type.Int));
        actual = ce.eval(input, context);
        assertSame(input.getColumn(0), actual);

        ce = new CastExpression(ce("float"), ResolvedType.of(Type.Int));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(Type.Int, 1, null, 4, 5), actual);

        try
        {
            ce = new CastExpression(ce("string"), ResolvedType.of(Type.Int));
            actual = ce.eval(input, context);
            assertVectorsEquals(vv(Type.Int, 1, null, 4, 5), actual);
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast 'one' to Int"));
        }

        ce = new CastExpression(ce("boolean"), ResolvedType.of(Type.Int));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(Type.Int, 1, 0, 1, null), actual);

        ce = new CastExpression(ce("long"), ResolvedType.of(Type.Int));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(Type.Int, 100, 200, 300, 400), actual);

        try
        {
            ce = new CastExpression(ce("datetime"), ResolvedType.of(Type.Int));
            actual = ce.eval(input, context);
            assertVectorsEquals(vv(Type.Int, 100, 200, 300, 400), actual);
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot cast type DateTime to Int"));
        }

        ce = new CastExpression(ce("stringInt"), ResolvedType.of(Type.Int));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(Type.Int, 1, 2, null, 4), actual);

        // Cast to array
        ce = new CastExpression(ce("stringInt"), ResolvedType.array(Type.Any));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(ResolvedType.of(Type.String), "1"), vv(ResolvedType.of(Type.String), "2"), null, vv(ResolvedType.of(Type.String), "4")), actual);

        // Cast to value vector
        ce = new CastExpression(ce("stringInt"), ResolvedType.array(Type.Any));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(ResolvedType.of(Type.String), "1"), vv(ResolvedType.of(Type.String), "2"), null, vv(ResolvedType.of(Type.String), "4")), actual);

        // Cast to value vector
        ce = new CastExpression(ce("anyValueVector"), ResolvedType.array(Type.Any));
        actual = ce.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.array(Type.Any), vv(ResolvedType.of(Type.Int), 1), vv(ResolvedType.of(Type.Long), 2L), null, vv(ResolvedType.of(Type.String), "hello")), actual);
    }
}
