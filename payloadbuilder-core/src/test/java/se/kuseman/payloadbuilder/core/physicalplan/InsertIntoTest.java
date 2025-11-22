package se.kuseman.payloadbuilder.core.physicalplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
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

        // Setut a runtime schema that should be used to verify schema rewriting
        context.getStatementContext()
                .setRuntimeSchema(1, expectedSchema);

        IPhysicalPlan plan = Mockito.mock(IPhysicalPlan.class);
        TupleIterator iterator = Mockito.mock(TupleIterator.class);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(vector);
        // Asterisk schema of input
        when(plan.getSchema()).thenReturn(Schema.of(ast("*", table)));
        when(plan.execute(context)).thenReturn(iterator);

        IDatasink sink = Mockito.mock(IDatasink.class);
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable
            {
                // Call iterator methods to verify that the lazy iterator catches them
                ((TupleIterator) invocation.getArgument(1)).hasNext();
                TupleVector vector = ((TupleIterator) invocation.getArgument(1)).next();
                ((TupleIterator) invocation.getArgument(1)).estimatedBatchCount();
                ((TupleIterator) invocation.getArgument(1)).estimatedRowCount();

                assertEquals(expectedSchema, vector.getSchema());
                assertEquals(3, vector.getRowCount());
                VectorTestUtils.assertVectorsEquals(ValueVector.literalInt(10, 3), vector.getColumn(0));

                // We skip close to make sure that InsertInto does that for us if implementation forget
                return null;
            }
        }).when(sink)
                .execute(any(IExecutionContext.class), any(TupleIterator.class));

        InsertInto into = new InsertInto(0, plan, null, sink);

        into.execute(context);

        verify(sink).execute(any(IExecutionContext.class), any(TupleIterator.class));
        verify(plan).getSchema();
        verify(plan).execute(context);
        verify(iterator).hasNext();
        verify(iterator).next();
        verify(iterator).estimatedBatchCount();
        verify(iterator).estimatedRowCount();
        verify(iterator).close();
        verifyNoMoreInteractions(plan, sink, iterator);
    }
}
