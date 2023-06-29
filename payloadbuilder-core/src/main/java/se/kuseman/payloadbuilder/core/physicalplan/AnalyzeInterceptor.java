package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Analyze interceptor that measures things like time spent, execution count. etc. */
public class AnalyzeInterceptor implements IPhysicalPlan
{
    private final IPhysicalPlan input;
    private final int nodeId;

    public AnalyzeInterceptor(int nodeId, IPhysicalPlan input)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        // We skip this node in tree because we want the query plan to look like if
        // these operators did not exist
        return input.getChildNodes();
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public String getName()
    {
        return input.getName();
    }

    @Override
    public int getNodeId()
    {
        return input.getNodeId();
    }

    @Override
    public int getActualNodeId()
    {
        return nodeId;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = input.getDescribeProperties(context);
        if (properties.isEmpty())
        {
            properties = new LinkedHashMap<>();
        }

        NodeData data = context.getStatementContext()
                .getNodeData(nodeId);
        populateTimings(context, data, input, properties);
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final AnalyzeData data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, AnalyzeData::new);
        data.increaseExecutionCount();

        if (data.iterator == null)
        {
            data.iterator = new AnalyzeIterator(data);
        }

        data.resumeNodeTime();
        final TupleIterator it = input.execute(context);
        data.suspenNodeTime();

        data.iterator.it = it;

        return data.iterator;
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof AnalyzeInterceptor)
        {
            AnalyzeInterceptor that = (AnalyzeInterceptor) obj;
            return nodeId == that.nodeId
                    && input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Analyze Interceptor " + "(" + nodeId + ")";
    }

    /** Skip interceptors in the tree to avoid verbosity */
    @Override
    public String print(int indent)
    {
        return input.print(indent);
    }

    static void populateTimings(IExecutionContext context, NodeData data, IPhysicalPlan input, Map<String, Object> properties)
    {
        long totalAcc = 0;
        long total = 0;
        float percentageTime = 0;
        int executionCount = 0;
        long rowCount = 0;

        if (data != null)
        {
            executionCount = data.getExecutionCount();
            rowCount = data.getRowCount();
            total = data.getNodeTime(TimeUnit.MILLISECONDS);
            totalAcc = total;
            // Remove the child times to get the actual time spent in this node
            List<IPhysicalPlan> children = input.getChildren();
            for (IPhysicalPlan child : children)
            {
                NodeData childData = context.getStatementContext()
                        .getNodeData(child.getActualNodeId());
                if (childData != null)
                {
                    total -= childData.getNodeTime(TimeUnit.MILLISECONDS);
                }
            }

            percentageTime = 100 * ((float) total / data.getTotalQueryTime());
        }

        // Append statistics to target nodes properties
        String timeSpent = String.format("%s (%.2f %%)", DurationFormatUtils.formatDurationHMS(Math.max(0, total)), percentageTime);
        properties.put(DescribeUtils.TIME_SPENT_ACC, DurationFormatUtils.formatDurationHMS(totalAcc));
        properties.put(DescribeUtils.TIME_SPENT, timeSpent);
        properties.put(DescribeUtils.EXECUTION_COUNT, executionCount);
        properties.put(DescribeUtils.PROCESSED_ROWS, rowCount);
    }

    /** Analyze iterator */
    private static class AnalyzeIterator implements TupleIterator
    {
        private final NodeData data;
        private TupleIterator it;

        AnalyzeIterator(NodeData data)
        {
            this.data = data;
        }

        @Override
        public TupleVector next()
        {
            data.resumeNodeTime();
            TupleVector vector = it.next();
            data.suspenNodeTime();
            data.increaseRowCount(vector.getRowCount());
            return vector;
        }

        @Override
        public boolean hasNext()
        {
            data.resumeNodeTime();
            boolean result = it.hasNext();
            data.suspenNodeTime();
            return result;
        }

        @Override
        public void close()
        {
            data.resumeNodeTime();
            it.close();
            data.suspenNodeTime();
        }
    }

    /** Analyze data */
    private static class AnalyzeData extends NodeData
    {
        AnalyzeIterator iterator;
    }
}
