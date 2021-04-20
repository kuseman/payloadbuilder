package org.kuse.payloadbuilder.core.operator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.operator.StatementContext.NodeData;

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

        NodeData data = context.getStatementContext().getNodeData(nodeId);
        if (data != null)
        {
            executionCount = data.getExecutionCount();
            rowCount = data.getRowCount();
            total = data.getNodeTime(TimeUnit.MILLISECONDS);
            totalAcc = total;
            // Remove the children times to get the actual time spent in this node
            List<DescribableNode> children = getChildNodes();
            for (DescribableNode child : children)
            {
                NodeData childData = context.getStatementContext().getNodeData(child.getActualNodeId());
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
        final AnalyzeData data = context.getStatementContext().getOrCreateNodeData(nodeId, AnalyzeData::new);
        data.increaseExecutionCount();

        if (data.iterator == null)
        {
            data.iterator = new AnalyzeIterator(data);
        }

        data.resumeNodeTime();
        final RowIterator it = target.open(context);
        data.suspenNodeTime();

        data.iterator.it = it;
        data.iterator.index = 0;

        return data.iterator;
    }

    /** Analyze iterator */
    private static class AnalyzeIterator implements RowIterator
    {
        private final NodeData data;
        private RowIterator it;
        private int index;

        AnalyzeIterator(NodeData data)
        {
            this.data = data;
        }

        @Override
        public Tuple next()
        {
            RowList list = it instanceof RowList ? (RowList) it : null;
            data.increaseRowCount();
            data.resumeNodeTime();
            Tuple result = list == null ? it.next() : list.get(index++);
            data.suspenNodeTime();
            return result;
        }

        @Override
        public boolean hasNext()
        {
            RowList list = it instanceof RowList ? (RowList) it : null;
            data.resumeNodeTime();
            boolean result = list == null ? it.hasNext() : index < list.size();
            data.suspenNodeTime();
            return result;
        }

        @Override
        public void close()
        {
            it.close();
        }
    }

    /** Analyze data */
    private static class AnalyzeData extends NodeData
    {
        AnalyzeIterator iterator;
    }
}
