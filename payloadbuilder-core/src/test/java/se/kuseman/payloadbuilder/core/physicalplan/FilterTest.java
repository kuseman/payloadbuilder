package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;

/** Test of {@link Filter} */
public class FilterTest extends APhysicalPlanTest
{
    @Test
    public void test_no_matches()
    {
        MutableBoolean closed = new MutableBoolean();

        IExpression col1 = ce("col1");
        IExpression col2 = ce("col2");
        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, col1, col2);

        Schema schema = schema("col1", "col2");

        IPhysicalPlan plan = new Filter(1,
                scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6)))), table, Schema.EMPTY),
                (tv, ctx) -> predicate.eval(tv, ctx));
        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleIterator it = plan.execute(context);
        assertFalse(it.hasNext());
        it.close();
        assertTrue(closed.getValue());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }
    }

    @Test
    public void test()
    {
        MutableBoolean closed = new MutableBoolean();

        IExpression col1 = ce("col1");
        IExpression col2 = ce("col2");
        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, col1, col2);

        Schema schema = schema("col1", "col2");

        IPhysicalPlan plan = new Filter(1, scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3, 4, 5), vv(Type.Any, 4, 5, 3, 2, 1)))), table, Schema.EMPTY),
                (tv, ctx) -> predicate.eval(tv, ctx));
        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);
        assertEquals(Schema.of(CoreColumn.of(table.column("col1"), ResolvedType.of(Type.Any)), CoreColumn.of(table.column("col2"), ResolvedType.of(Type.Any))), actual.getSchema());

        assertEquals(3, actual.getRowCount());
        assertVectorsEquals(vv(Type.Any, 3, 4, 5), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3, 2, 1), actual.getColumn(1));
        assertTrue(closed.getValue());
    }

    @Test
    public void test_outer_reference_is_preserved_until_evaluation()
    {
        TupleVector tv = TupleVector.of(schema("col1", "col3"), asList(vv(Type.Any, 3, 4), vv(Type.Any, 5, 3)));

        List<ValueVector> outerVectors = new ArrayList<>();

        IExpression col1 = ce("col1");
        IExpression ocol4 = new ColumnExpression("col4", "col4", ResolvedType.of(Type.Any), null, 0, true, -1)
        {
            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                ValueVector result = super.eval(input, context);
                outerVectors.add(result);
                return result;
            }
        };

        MutableBoolean closed = new MutableBoolean(false);
        IPhysicalPlan plan = new Filter(1, scan(schemaDS(() -> closed.setTrue(), tv), table, Schema.EMPTY), new ExpressionPredicate(eq(col1, ocol4)));

        Schema outerSchema = schema("col4");

        for (int i = 0; i < 3; i++)
        {
            context.getStatementContext()
                    .setOuterTupleVector(TupleVector.of(outerSchema, asList(vv(Type.Any, i, i))));
            TupleIterator iterator = plan.execute(context);
            TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), iterator);

            // Traverse vectors to trigg evaluation
            for (int c = 0; c < actual.getSchema()
                    .getSize(); c++)
            {
                for (int r = 0; r < actual.getRowCount(); r++)
                {
                    actual.getColumn(c)
                            .getAny(r);
                }
            }
        }

        assertVectorsEquals(outerVectors.get(0), vv(Type.Any, 0, 0));
        assertVectorsEquals(outerVectors.get(1), vv(Type.Any, 1, 1));
        assertVectorsEquals(outerVectors.get(2), vv(Type.Any, 2, 2));
    }
}
