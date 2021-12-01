package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.kuse.payloadbuilder.core.operator.StatementContext.NodeData;

/**
 * Operator used along with Batch-operators when Parallelism is wanted
 *
 * <pre>
 * Makes a batch join parallel by splitting up the batches and opening those in an executor
 *
 * Operator tree looks like this:
 *
 * Ordinary batch hash join
 *
 * BatchHashJoin
 *   outer
 *   inner
 *
 * When parallelism is applied the tree lokks like this:
 *
 * BatchParallelism
 *   outer
 *   outerListNodeId
 *   BatchHashJoin
 *     outerList    (Operator that stream rows from node context set by BatchParallelism operator)
 *     inner
 * </pre>
 *
 * NOTE! Experimental status
 */
class BatchParallelismOperator extends AOperator
{
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    /** The outer operator of the batch op */
    private final Operator outer;
    /** The batch operator */
    private final Operator inner;
    /** The node id of the outer operator that is used by the batch operator */
    private final Integer targetOuterNodeId;
    /** Batch size */
    private final int batchSize;

    BatchParallelismOperator(
            int nodeId,
            Operator outer,
            Operator inner,
            int targetOuterNodeId,
            int batchSize)
    {
        super(nodeId);
        this.batchSize = batchSize;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.targetOuterNodeId = targetOuterNodeId;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(inner);
    }

    private ExecutionContext startBatch(ArrayBlockingQueue<Tuple> queue, ExecutionContext parent, List<Tuple> outerBatch, AtomicBoolean abort, AtomicInteger latch)
    {
        ExecutionContext childContext = parent.copy();
        BatchParallelismNodeData nodeData = childContext.getStatementContext().getOrCreateNodeData(targetOuterNodeId, BatchParallelismNodeData::new);
        nodeData.batch = outerBatch;
        EXECUTOR.submit(() ->
        {
            TupleIterator it = inner.open(childContext);
            while (it.hasNext())
            {
                if (abort.get())
                {
                    break;
                }

                try
                {
                    queue.put(it.next());
                }
                catch (InterruptedException e)
                {
                    throw new OperatorException("Erro while putting to queue", e);
                }
            }
            it.close();
            latch.decrementAndGet();
        });
        return childContext;
    }

    @Override
    public TupleIterator open(final ExecutionContext context)
    {
        final ArrayBlockingQueue<Tuple> queue = new ArrayBlockingQueue<>(1000);
        final AtomicInteger latch = new AtomicInteger();
        final AtomicBoolean abort = new AtomicBoolean();
        final List<ExecutionContext> childContexts = new ArrayList<>();
        List<Tuple> batch = new ArrayList<>(batchSize);
        TupleIterator it = outer.open(context);
        while (it.hasNext())
        {
            batch.add(it.next());
            if (batch.size() == batchSize || !it.hasNext())
            {
                latch.incrementAndGet();
                childContexts.add(startBatch(queue, context, batch, abort, latch));
                batch = new ArrayList<>(batchSize);
            }
        }
        it.close();

        //CSOFF
        return new TupleIterator()
        //CSON
        {
            @Override
            public Tuple next()
            {
                try
                {
                    return queue.take();
                }
                catch (InterruptedException e)
                {
                    throw new OperatorException("Error while taking from queue", e);
                }
            }

            @Override
            public boolean hasNext()
            {
                return abort.get() || latch.get() > 0 || queue.size() > 0;
            }

            @Override
            public void close()
            {
                abort.set(true);
                queue.clear();
                // Put a last tuple in queue to let go take call in next()
                queue.offer(NoOpTuple.NO_OP);

                for (ExecutionContext childContext : childContexts)
                {
                    context.getStatementContext().mergeNodeData(childContext.getStatementContext());
                }
            }
        };
    }

    /** Node data for target outer operator. */
    static class BatchParallelismNodeData extends NodeData
    {
        List<Tuple> batch;
    }

    /**
     * <pre>
     * Outer operator used in batch operator when parallelism is wanted.
     * This operator simply streams the batched outer rows set to context by
     * {@link BatchParallelismOperator}
     * </pre>
     */
    static class BatchhParallelismOuterOperator extends AOperator
    {
        BatchhParallelismOuterOperator(int nodeId)
        {
            super(nodeId);
        }

        @Override
        public TupleIterator open(ExecutionContext context)
        {
            final BatchParallelismNodeData data = context.getStatementContext().getNodeData(nodeId);
            return new TupleIterator()
            {
                int index;

                @Override
                public Tuple next()
                {
                    return data.batch.get(index++);
                }

                @Override
                public boolean hasNext()
                {
                    return index < data.batch.size();
                }
            };
        }
    }
}
