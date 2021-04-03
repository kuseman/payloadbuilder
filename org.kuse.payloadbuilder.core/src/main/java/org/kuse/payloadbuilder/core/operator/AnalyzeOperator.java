package org.kuse.payloadbuilder.core.operator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Analyze operator that measures things like time spent, execution count. etc. */
class AnalyzeOperator extends AOperator
{
    private final Operator target;

    AnalyzeOperator(int nodeId, Operator target)
    {
        super(nodeId);
        this.target = target;
    }

    @Override
    public List<Operator> getChildOperators()
    {
        // We skip this node in tree because we want the query plan to look like if
        // these operators did not exist
        return target.getChildOperators();
    }

    @Override
    public String getName()
    {
        return target.getName();
    }

    @Override
    public String getDescribeString()
    {
        return target.getDescribeString();
    }

    @Override
    public int getNodeId()
    {
        return target.getNodeId();
    }

    @Override
    public int getActualNodeId()
    {
        return nodeId;
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
        long rowCount = 0;

        NodeData data = context.getOperatorContext().getNodeData(nodeId);
        if (data != null)
        {
            executionCount = data.getExecutionCount();
            rowCount = data.getRowCount();
            total = data.getNodeTime(TimeUnit.MILLISECONDS);
            totalAcc = total;
            // Remove the children times to get the actual time spent in this node
            List<Operator> children = getChildOperators();
            for (Operator child : children)
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
        properties.put(DescribeUtils.PROCESSED_ROWS, rowCount);

        return properties;
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        final NodeData data = context.getOperatorContext().getNodeData(nodeId, NodeData::new);
        data.increaseExecutionCount();
        data.resumeNodeTime();
        final RowIterator it = target.open(context);
        data.suspenNodeTime();
        //CSOFF
        return new RowIterator()
        //CSON
        {
            @Override
            public Tuple next()
            {
                data.increaseRowCount();
                data.resumeNodeTime();
                Tuple result = it.next();
                data.suspenNodeTime();
                return result;
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
                it.close();
            }
        };
    }
}
