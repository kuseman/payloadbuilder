package se.kuseman.payloadbuilder.core.physicalplan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;

/** Test if {@link InsertInto} */
public class InserIntoTest extends APhysicalPlanTest
{
    @Test
    public void test_that_input_is_closed()
    {
        IPhysicalPlan plan = Mockito.mock(IPhysicalPlan.class);
        TupleIterator iterator = Mockito.mock(TupleIterator.class);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(TupleVector.EMPTY);
        when(plan.execute(context)).thenReturn(iterator);

        IDatasink sink = Mockito.mock(IDatasink.class);
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable
            {
                // Call iterator methods to verify that the lazy iterator catches them
                ((TupleIterator) invocation.getArgument(1)).hasNext();
                ((TupleIterator) invocation.getArgument(1)).next();
                ((TupleIterator) invocation.getArgument(1)).estimatedBatchCount();
                ((TupleIterator) invocation.getArgument(1)).estimatedRowCount();

                // We skip close to make sure that InsertInto does that for us if implementation forgett
                return null;
            }
        }).when(sink)
                .execute(any(IExecutionContext.class), any(TupleIterator.class));

        InsertInto into = new InsertInto(0, plan, null, sink);

        into.execute(context);

        verify(sink).execute(any(IExecutionContext.class), any(TupleIterator.class));
        verify(plan).execute(context);
        verify(iterator).hasNext();
        verify(iterator).next();
        verify(iterator).estimatedBatchCount();
        verify(iterator).estimatedRowCount();
        verify(iterator).close();
        verifyNoMoreInteractions(plan, sink, iterator);
    }
}
