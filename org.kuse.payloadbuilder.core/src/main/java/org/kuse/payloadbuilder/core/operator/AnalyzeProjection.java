package org.kuse.payloadbuilder.core.operator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;

/** Analyze projection that measures things like time spent, execution count. etc. */
class AnalyzeProjection implements Projection
{
    private final Projection target;
    private final int nodeId;

    AnalyzeProjection(int nodeId, Projection target)
    {
        this.nodeId = nodeId;
        this.target = target;
    }

    Projection getTarget()
    {
        return target;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        // We skip this node in tree because we want the query plan to look like if
        // these operators did not exist
        return target.getChildNodes();
    }

    @Override
    public String getName()
    {
        return target.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        Map<String, Object> properties = target.getDescribeProperties(context);
        if (properties.isEmpty())
        {
            properties = new LinkedHashMap<>();
        }

        long totalAcc = 0;
        long total = 0;
        float percentageTime = 0;
        int executionCount = 0;

        NodeData data = context.getOperatorContext().getNodeData(nodeId);
        if (data != null)
        {
            executionCount = data.getExecutionCount();
            total = data.getNodeTime(TimeUnit.MILLISECONDS);
            totalAcc = total;
            // Remove the children times to get the actual time spent in this node
            List<DescribableNode> children = getChildNodes();
            for (DescribableNode child : children)
            {
                NodeData childData = context.getOperatorContext().getNodeData(child.getActualNodeId());
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

        return properties;
    }

    @Override
    public boolean isAsterisk()
    {
        return target.isAsterisk();
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        final NodeData data = context.getOperatorContext().getNodeData(nodeId, NodeData::new);
        data.increaseExecutionCount();
        data.resumeNodeTime();
        target.writeValue(writer, context);
        data.suspenNodeTime();
    }
}
