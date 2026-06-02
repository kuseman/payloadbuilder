package se.kuseman.payloadbuilder.core.physicalplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test if {@link InsertInto} */
class InsertIntoTest extends APhysicalPlanTest
{
    @Test
    void test_that_input_is_closed()
    {
        TableSourceReference table = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");

        Schema vectorSchema = Schema.of(col("col1", ResolvedType.INT, table));
        TupleVector vector = TupleVector.of(vectorSchema, ValueVector.literalInt(10, 3));

        Schema expectedSchema = Schema.of(CoreColumn.Builder.from("col1", ResolvedType.INT)
                .withColumnReference(new ColumnReference("col1", table, Column.MetaData.EMPTY))
                .withMetaData(new Column.MetaData(Map.of("key", "value")))
                .build());

        // Set a runtime schema that should be used to verify schema rewriting
        context.getStatementContext()
                .setRuntimeSchema(1, expectedSchema);

        IPhysicalPlan plan = Mockito.mock(IPhysicalPlan.class);
        TupleIterator iterator = Mockito.mock(TupleIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(vector);
        when(iterator.estimatedBatchCount()).thenReturn(3);
        when(iterator.estimatedRowCount()).thenReturn(100);
        // Asterisk schema of input
        when(plan.getSchema()).thenReturn(Schema.of(ast("*", table)));
        when(plan.execute(context)).thenReturn(iterator);

        // The sink receives a Supplier — call input.get() to execute the upstream plan,
        // then consume the resulting iterator. InsertInto must close the iterator.
        TupleVector[] captured = new TupleVector[1];
        IDatasink sink = (ctx, input) ->
        {
            TupleIterator it = input.get();
            // Verify wrapper forwards estimation methods to the underlying iterator
            assertEquals(3, it.estimatedBatchCount());
            assertEquals(100, it.estimatedRowCount());
            it.hasNext();
            captured[0] = it.next();
            // Intentionally skip close() to verify InsertInto closes it
        };

        InsertInto into = new InsertInto(0, plan, null, sink);
        into.execute(context);

        // Verify the proxy vector applied the asterisk schema rewrite
        assertEquals(expectedSchema, captured[0].getSchema());
        assertEquals(3, captured[0].getRowCount());
        VectorTestUtils.assertVectorsEquals(ValueVector.literalInt(10, 3), captured[0].getColumn(0));

        verify(plan).getSchema();
        verify(plan).execute(context);
        verify(iterator).estimatedBatchCount();
        verify(iterator).estimatedRowCount();
        verify(iterator).hasNext();
        verify(iterator).next();
        verify(iterator).close();
        verifyNoMoreInteractions(plan, iterator);
    }

    @Test
    void test_sink_can_skip_execution_on_cache_hit()
    {
        IPhysicalPlan plan = Mockito.mock(IPhysicalPlan.class);
        when(plan.getSchema()).thenReturn(Schema.EMPTY);

        // Sink that does NOT call input.get() — simulates a cache hit
        IDatasink sink = (ctx, input) ->
        {
        };

        InsertInto into = new InsertInto(0, plan, null, sink);
        into.execute(context);

        // Plan was never executed since sink did not call input.get()
        verify(plan).getSchema();
        verifyNoMoreInteractions(plan);
    }
}
