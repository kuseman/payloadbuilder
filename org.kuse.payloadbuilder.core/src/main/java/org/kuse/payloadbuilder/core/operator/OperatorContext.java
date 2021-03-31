package org.kuse.payloadbuilder.core.operator;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.time.StopWatch;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Stores node unique data by node's unique id */
    private final Map<Integer, NodeData> nodeDataById = new ConcurrentHashMap<>();

    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<Object[]> outerIndexValues;

    public Iterator<Object[]> getOuterIndexValues()
    {
        return outerIndexValues;
    }

    public void setOuterIndexValues(Iterator<Object[]> outerIndexValues)
    {
        this.outerIndexValues = outerIndexValues;
    }

    /** Clear context state */
    public void clear()
    {
        outerIndexValues = null;
        nodeDataById.clear();
    }

    public Map<Integer, ? extends NodeData> getNodeData()
    {
        return nodeDataById;
    }

    /** Get node data by id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId)
    {
        return (T) nodeDataById.get(nodeId);
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId, Supplier<T> creator)
    {
        return (T) nodeDataById.computeIfAbsent(nodeId, k -> creator.get());
    }

    /** Base class for node data. */
    //CSOFF
    public static class NodeData
    {
        private final StopWatch nodeTime = new StopWatch();
        private int executionCount;
        private long rowCount;
        /** <pre> 
         * Time that is set on all nodes after query is run to be 
         *  able to calculate a percentage when {@link Operator#getDescribeProperties} is called 
         *  </pre>
         */
        private long totalQueryTime;
        
        protected NodeData()
        {
            nodeTime.start();
            nodeTime.suspend();
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
