package se.kuseman.payloadbuilder.api.execution;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import se.kuseman.payloadbuilder.api.utils.StopWatch;

/** Base class for node data. */
public class NodeData
{
    private final StopWatch nodeTime = new StopWatch();
    private Duration nodeTimeDuration;
    private int executionCount;
    private long rowCount;
    private long batchCount;

    /**
     * <pre>
     *
      * Time that is set on all nodes after query is run to be
      *  able to calculate a percentage when {@link Operator#getDescribeProperties} is called
     * </pre>
     */
    private long totalQueryTime;

    public NodeData()
    {
        nodeTime.start();
    }

    /** Merge this data with provided */
    public void merge(NodeData nodeData)
    {
        executionCount += nodeData.executionCount;
        rowCount += nodeData.rowCount;
        if (nodeTimeDuration == null)
        {
            nodeTimeDuration = Duration.of(nodeTime.getElapsedMilliseconds(), ChronoUnit.MILLIS);
        }
        else if (nodeData.nodeTimeDuration == null)
        {
            nodeTimeDuration = nodeTimeDuration.plus(Duration.of(nodeData.nodeTime.getElapsedMilliseconds(), ChronoUnit.MILLIS));
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
        nodeTime.start();
    }

    /** Suspend node time stop watch */
    public void suspenNodeTime()
    {
        nodeTime.stop();
    }

    /** Return the node time */
    public long getNodeTime(TimeUnit timeUnit)
    {
        if (nodeTimeDuration != null)
        {
            return timeUnit.convert(nodeTimeDuration.toMillis(), TimeUnit.MILLISECONDS);
        }
        return timeUnit.convert(nodeTime.getElapsedMilliseconds(), TimeUnit.MILLISECONDS);
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

    public void increaseRowCount(int delta)
    {
        rowCount += delta;
    }

    public long getBatchCount()
    {
        return batchCount;
    }

    public void increaseBatchCount()
    {
        batchCount++;
    }
}