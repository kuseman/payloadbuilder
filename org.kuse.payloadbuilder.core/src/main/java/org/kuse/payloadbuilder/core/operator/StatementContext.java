package org.kuse.payloadbuilder.core.operator;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory.IIndexValues;

/**
 * Context used during a statement.
 *
 * <pre>
 * Contains data that is used during a select etc.
 *
 * Life cycle here is only during one statement, is cleared
 * after completion.
 * </pre>
 */
public class StatementContext
{
    /** Stores node unique data by node's unique id */
    private final Map<Integer, NodeData> nodeDataById = new ConcurrentHashMap<>();
    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<IIndexValues> outerIndexValues;
    /** Holder for lambda references during evaluation */
    private List<Object> lambdaValues;
    private ZonedDateTime now = ZonedDateTime.now();
    /** Current row count of previous select statement */
    private int rowCount;
    /** Reference to current context tuple. Used when evaluating expression through out a query */
    private Tuple tuple;

    public StatementContext()
    {
    }

    StatementContext(StatementContext source)
    {
        this.tuple = source.tuple;
    }

    public Iterator<IIndexValues> getOuterIndexValues()
    {
        return outerIndexValues;
    }

    public void setOuterIndexValues(Iterator<IIndexValues> outerIndexValues)
    {
        this.outerIndexValues = outerIndexValues;
    }

    /** Consumes any left outer values */
    public void consumeOuterValues()
    {
        if (outerIndexValues != null)
        {
            while (outerIndexValues.hasNext())
            {
                outerIndexValues.next();
            }
        }
    }

    /** Clear context state */
    public void clear()
    {
        rowCount = 0;
        tuple = null;
        now = ZonedDateTime.now();
        lambdaValues = null;
        outerIndexValues = null;
        nodeDataById.clear();
    }

    public Map<Integer, ? extends NodeData> getNodeData()
    {
        return nodeDataById;
    }

    /** Get node data by id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(Integer nodeId)
    {
        return (T) nodeDataById.get(nodeId);
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getOrCreateNodeData(Integer nodeId, final Supplier<T> creator)
    {
        // NOTE! If using computeIfAbsent here the calls from AnalyzeOperator/AnalyzeProjection
        // causes ALOT of allocations of LambdaForm's when flight recording, cannot understand why.
        // Might be related to hot code since analyze code is called much more often
        // So this is not thread safe atm. Might introduce locking and change to an array based solution instead
        T res = (T) nodeDataById.get(nodeId);
        if (res == null)
        {
            res = creator.get();
            nodeDataById.put(nodeId, res);
        }
        return res;
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getOrCreateNodeData(Integer nodeId)
    {
        return (T) nodeDataById.computeIfAbsent(nodeId, k -> new NodeData());
    }

    /** Get current tuple */
    public Tuple getTuple()
    {
        return tuple;
    }

    /** Set current tuple */
    public void setTuple(Tuple tuple)
    {
        this.tuple = tuple;
    }

    /** Return current time in local time */
    public ZonedDateTime getNow()
    {
        return now;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(int rowCount)
    {
        this.rowCount = rowCount;
    }

    /** Get lambda value in scope for provided id */
    public Object getLambdaValue(int lambdaId)
    {
        if (lambdaValues == null)
        {
            return null;
        }
        ensureSize(lambdaValues, lambdaId);
        return lambdaValues.get(lambdaId);
    }

    /** Set lambda value in scope for provided id */
    public void setLambdaValue(int lambdaId, Object value)
    {
        if (lambdaValues == null)
        {
            lambdaValues = new ArrayList<>();
        }
        ensureSize(lambdaValues, lambdaId);
        lambdaValues.set(lambdaId, value);
    }

    /** Merge this context's node data with provided context's */
    public void mergeNodeData(StatementContext context)
    {
        // Merge nodes
        for (Entry<Integer, NodeData> e : context.nodeDataById.entrySet())
        {
            NodeData existing = nodeDataById.get(e.getKey());
            if (existing != null)
            {
                existing.merge(e.getValue());
            }
            else
            {
                nodeDataById.put(e.getKey(), e.getValue());
            }
        }
    }

    private void ensureSize(List<?> list, int itemIndex)
    {
        // size = 2, index = 0, 1
        int size = list.size();
        if (size > itemIndex)
        {
            return;
        }

        // size 2, index = 2
        int diff = itemIndex + 1 - size;
        list.addAll(Collections.nCopies(diff, null));
    }

    /** Base class for node data. */
    //CSOFF
    public static class NodeData
    {
        private final StopWatch nodeTime = new StopWatch();
        private Duration nodeTimeDuration;
        private int executionCount;
        private long rowCount;
        /**
         * <pre>
         *
          * Time that is set on all nodes after query is run to be
          *  able to calculate a percentage when {@link Operator#getDescribeProperties} is called
         * </pre>
         */
        private long totalQueryTime;

        protected NodeData()
        {
            nodeTime.start();
            nodeTime.suspend();
        }

        protected void merge(NodeData nodeData)
        {
            executionCount += nodeData.executionCount;
            rowCount += nodeData.rowCount;
            if (nodeTimeDuration == null)
            {
                nodeTimeDuration = Duration.of(nodeTime.getTime(TimeUnit.MILLISECONDS), ChronoUnit.MILLIS);
            }
            else if (nodeData.nodeTimeDuration == null)
            {
                nodeTimeDuration = nodeTimeDuration.plus(Duration.of(nodeData.nodeTime.getTime(TimeUnit.MILLISECONDS), ChronoUnit.MILLIS));
            }
            else
            {
                nodeTimeDuration = nodeTimeDuration.plus(nodeData.nodeTimeDuration);
            }
        }

        public long getTotalQueryTime()
        {
            return totalQueryTime;
        }

        public void setTotalQueryTime(long totalQueryTime)
        {
            this.totalQueryTime = totalQueryTime;
        }

        /** Resumes node time stop watch */
        public void resumeNodeTime()
        {
            nodeTime.resume();
        }

        /** Suspend node time stop watch */
        public void suspenNodeTime()
        {
            nodeTime.suspend();
        }

        /** Return the node time */
        public long getNodeTime(TimeUnit timeUnit)
        {
            if (nodeTimeDuration != null)
            {
                return timeUnit.convert(nodeTimeDuration.toMillis(), TimeUnit.MILLISECONDS);
            }
            return nodeTime.getTime(timeUnit);
        }

        public int getExecutionCount()
        {
            return executionCount;
        }

        /** Increase execution count */
        public void increaseExecutionCount()
        {
            executionCount++;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public void increaseRowCount()
        {
            rowCount++;
        }
    }
    //CSON
}
