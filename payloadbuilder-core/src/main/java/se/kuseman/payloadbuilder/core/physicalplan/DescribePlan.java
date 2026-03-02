package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.OutputWriterUtils;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeVisitor.AnalyzeFormat;

/** Plan for describing another physical plan and return describable output */
public class DescribePlan implements IPhysicalPlan
{
    private static final NoOpOutputWriter WRITER = new NoOpOutputWriter();
    private final int nodeId;
    private final IPhysicalPlan input;
    private final boolean analyze;
    private final String queryText;
    private final AnalyzeFormat format;

    public DescribePlan(int nodeId, IPhysicalPlan input, boolean analyze, AnalyzeFormat format, String queryText)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.analyze = analyze;
        this.format = requireNonNull(format, "format");
        this.queryText = queryText;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    public IPhysicalPlan getInput()
    {
        return input;
    }

    public boolean isAnalyze()
    {
        return analyze;
    }

    public AnalyzeFormat getAnalyzeFormat()
    {
        return format;
    }

    public String getQueryText()
    {
        return queryText;
    }

    @Override
    public Schema getSchema()
    {
        return Schema.EMPTY;
    }

    @Override
    public String getName()
    {
        return "Write Output";
    }

    @Override
    public <T, C> T accept(IPhysicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("Query Text", queryText);

        if (analyze)
        {
            NodeData data = context.getStatementContext()
                    .getNodeData(nodeId);
            AnalyzeInterceptor.populateTimings(context, data, this, properties);
            properties.put("Allocations", ((ExecutionContext) context).getBufferAllocator()
                    .getStatistics()
                    .asObject());

            Map<String, Map<String, Object>> catalogStatistics = new HashMap<>();
            QuerySession session = (QuerySession) context.getSession();
            session.getCatalogRegistry()
                    .getCatalogs()
                    .forEach(e ->
                    {
                        Map<String, Object> statistics = e.getValue()
                                .getExecutionStatistics(context);
                        if (statistics.isEmpty())
                        {
                            return;
                        }
                        catalogStatistics.put(e.getValue()
                                .getName(), statistics);
                    });

            properties.put("Catalog Statistics", catalogStatistics);
        }
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // Execute and traverse query in analyze mode before gathering describe data
        if (analyze)
        {
            // Trigger creation of node data to get this nodes timing in output
            NodeData data = context.getStatementContext()
                    .getOrCreateNodeData(nodeId);
            data.increaseExecutionCount();

            int rowCount = 0;
            long start = System.nanoTime();
            data.resumeNodeTime();
            TupleIterator it = input.execute(context);
            try
            {
                while (it.hasNext())
                {
                    TupleVector tv = it.next();
                    rowCount += tv.getRowCount();
                    OutputWriterUtils.write(tv, WRITER, context, true);
                }
            }
            finally
            {
                it.close();

                data.suspenNodeTime();
                data.increaseRowCount(rowCount);

                // Populate total query time on every node data
                long total = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                ((StatementContext) context.getStatementContext()).forEachNodeData((i, n) -> n.setTotalQueryTime(total));
            }
        }

        ((ExecutionContext) context).getStatementContext()
                .setOuterTupleVector(new DescribeTupleVector());
        return TupleIterator.singleton(DescribeUtils.getDescribeVector(context, format, this));
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
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
        else if (obj instanceof DescribePlan that)
        {
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && analyze == that.analyze;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (analyze ? "Analyze"
                : "Describe")
               + " Plan ("
               + nodeId
               + ")";
    }

    /** No op writer used to measure the execution of projection expressions */
    private static class NoOpOutputWriter implements OutputWriter
    {
        @Override
        public void writeFieldName(String name)
        {
        }

        @Override
        public void writeValue(Object value)
        {
        }

        @Override
        public void startObject()
        {
        }

        @Override
        public void endObject()
        {
        }

        @Override
        public void startArray()
        {
        }

        @Override
        public void endArray()
        {
        }
    }

    /**
     * Tuple vector used as outer reference during describe operation. Catalog implementations might evaluate expressions during describe and at that point a regular outer reference is not existent
     * and will yield a runtime error.
     */
    private static class DescribeTupleVector implements TupleVector
    {
        @Override
        public int getRowCount()
        {
            return 1;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return ValueVector.literalAny(1, "<outer value>");
        }

        @Override
        public Schema getSchema()
        {
            return Schema.of(Column.of("outerColumn", Type.Any));
        }
    }
}
