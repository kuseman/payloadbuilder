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
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link CaseExpression} */
public class CaseExpressionTest extends APhysicalPlanTest
{
    @Test
    public void test_type()
    {
        assertEquals(ResolvedType.of(Type.Int), e("case when 1 > 0 then 'hello' when 2 > 0 then 10 end").getType());
        assertEquals(ResolvedType.of(Type.String), e("case when 1 > 0 then 'hello' end").getType());
        assertEquals(ResolvedType.of(Type.Float), e("case when 1 > 0 then 10 else 10.0f end").getType());
        assertEquals(ResolvedType.of(Type.Int), e("case when 1 > 0 then 10 when false then true else 'hello' end").getType());
    }

    @Test
    public void test_where_not_all_rows_can_be_evaluated_agaisnt_else()
    {
        // Test that verifies expressions like this:
        // CASE WHEN value = '' THEN null ELSE CAST(value AS DATETIME) END
        // Here we cannot evaluate all rows against ELSE since then we are breaking contract

        CaseExpression e;
        ValueVector actual;

        IExpression col3 = ce("col3", ResolvedType.of(Type.String));

        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col3", ResolvedType.of(Type.String)));
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.String, "", "1900-01-01", "2010-10-10", "")
                ));
        //@formatter:on

        e = new CaseExpression(asList(new ICaseExpression.WhenClause(eq(col3, new LiteralStringExpression("")), new LiteralNullExpression(ResolvedType.of(Type.String)))),
                new CastExpression(col3, ResolvedType.of(Type.DateTime)));

        actual = e.eval(input, context);

        assertVectorsEquals(vv(ResolvedType.of(Type.DateTime), null, EpochDateTime.from("1900-01-01"), EpochDateTime.from("2010-10-10"), null), actual);
    }

    @Test
    public void test()
    {
        CaseExpression e;
        ValueVector actual;

        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));
        IExpression col2 = ce("col2", ResolvedType.of(Type.Float));
        IExpression col3 = ce("col3", ResolvedType.of(Type.String));
        IExpression col4 = ce("col4", ResolvedType.of(Type.Boolean));
        IExpression col5 = ce("col5", ResolvedType.of(Type.Long));

        //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.Int)),
                Column.of("col2", ResolvedType.of(Type.Float)),
                Column.of("col3", ResolvedType.of(Type.String)),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)));
                
        TupleVector input = TupleVector.of(schema, asList(
                vv(Type.Int, null, 2, 4, 5),
                vv(Type.Float, 1F, null, 4F, 5F),
                vv(Type.String, "one", "two", null, "four"),
                vv(Type.Boolean, true, false, true, null),
                vv(Type.Long, 100L, 200L, 300L, 400L)
                ));
        //@formatter:on

        e = new CaseExpression(asList(new ICaseExpression.WhenClause(gt(col1, col2), new LiteralStringExpression("first"))), null);
        assertEquals(ResolvedType.of(Type.String), e.getType());
        actual = e.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.String), null, null, null, null), actual);

        e = new CaseExpression(asList(new ICaseExpression.WhenClause(col4, add(col1, col2))), null);
        assertEquals(ResolvedType.of(Type.Float), e.getType());
        actual = e.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), null, null, 8.0F, null), actual);

        // Here we have a complext strucutre of return values (int + float) and string
        // Which ends up in a class cast
        try
        {
            e = new CaseExpression(asList(new ICaseExpression.WhenClause(eq(col1, col2), add(col1, col2))), col3);
            assertEquals(ResolvedType.of(Type.Float), e.getType());
            actual = e.eval(input, context);
            assertVectorsEquals(vv(ResolvedType.of(Type.Float), "one", "two", 8.0F, 10.0F), actual);
            fail("Should fail with class cast string -> number");
        }
        catch (IllegalArgumentException exp)
        {
            assertTrue(exp.getMessage(), exp.getMessage()
                    .contains("Cannot cast 'one' to Float"));
        }

        // Different value vectors for different rows
        // case when col1 = 4 then col2 when col1 = 5 then col5 end
        e = new CaseExpression(asList(new ICaseExpression.WhenClause(eq(col1, intLit(4)), col2), new CaseExpression.WhenClause(eq(col1, intLit(5)), col5)), null);
        assertEquals(ResolvedType.of(Type.Float), e.getType());
        actual = e.eval(input, context);
        assertVectorsEquals(vv(ResolvedType.of(Type.Float), null, null, 4F, 400F), actual);
    }
}
