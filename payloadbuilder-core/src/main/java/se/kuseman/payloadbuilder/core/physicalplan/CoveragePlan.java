package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.CaseCoverageData;
import se.kuseman.payloadbuilder.core.execution.LogicalBinaryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.BranchCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.ConditionCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.OperatorCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageRegistry;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Physical plan that executes a query with full ANALYZE instrumentation, buffers result rows, appends a {@code __coverage__} column with coverage JSON and notifies {@link QueryCoverageRegistry}
 * listeners. Injected by {@link se.kuseman.payloadbuilder.core.planning.StatementRewriter} when coverage is enabled.
 */
public class CoveragePlan implements IPhysicalPlan
{
    /** Name of the coverage metadata column appended to every result set when coverage is active. */
    public static final String COVERAGE_COLUMN = CoverageTupleVector.COVERAGE_COLUMN;

    private final int nodeId;
    private final IPhysicalPlan input;
    private final Map<Integer, Location> nodeLocations;
    private final String queryName;

    public CoveragePlan(int nodeId, IPhysicalPlan input, Map<Integer, Location> nodeLocations)
    {
        this(nodeId, input, nodeLocations, null);
    }

    public CoveragePlan(int nodeId, IPhysicalPlan input, Map<Integer, Location> nodeLocations, String queryName)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.nodeLocations = requireNonNull(nodeLocations, "nodeLocations");
        this.queryName = queryName;
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return Schema.EMPTY;
    }

    @Override
    public String getName()
    {
        return "Coverage";
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // 1. Execute inner plan (ANALYZE interceptors are active), buffer all rows
        List<TupleVector> buffer = new ArrayList<>();
        TupleIterator it = input.execute(context);
        try
        {
            while (it.hasNext())
            {
                buffer.add(it.next());
            }
        }
        finally
        {
            it.close();
        }

        // 2. Collect coverage data from StatementContext
        StatementContext stmtContext = (StatementContext) context.getStatementContext();
        List<OperatorCoverage> operators = collectOperatorCoverage(input, stmtContext, nodeLocations);
        List<BranchCoverage> branches = collectBranchCoverage(stmtContext);
        List<ConditionCoverage> conditions = collectConditionCoverage(stmtContext);

        String queryKey = queryName != null ? queryName
                : String.format("%08x", Math.abs(input.print(0)
                        .hashCode()));
        QueryCoverageData coverageData = new QueryCoverageData(queryKey, operators, branches, conditions);
        String coverageJson = coverageData.toJson();

        // 3. Notify listeners (JUnit extension etc.)
        QueryCoverageRegistry.notifyListeners(coverageData);

        // 4. Wrap buffered rows with coverage column and return
        if (buffer.isEmpty())
        {
            return TupleIterator.EMPTY;
        }

        List<TupleVector> covered = new ArrayList<>(buffer.size());
        for (TupleVector tv : buffer)
        {
            covered.add(new CoverageTupleVector(tv, coverageJson));
        }

        return new TupleIterator()
        {
            private int index = 0;

            @Override
            public TupleVector next()
            {
                return covered.get(index++);
            }

            @Override
            public boolean hasNext()
            {
                return index < covered.size();
            }
        };
    }

    @Override
    public boolean hasWritableOutput()
    {
        // Delegate so CoveragePlan(InsertInto) is treated as non-writable and auto-executed
        // by the query runner without requiring an explicit consumeResult() call from tests.
        return input.hasWritableOutput();
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
        if (obj instanceof CoveragePlan that)
        {
            return nodeId == that.nodeId
                    && input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Coverage Plan (" + nodeId + ")";
    }

    private static List<OperatorCoverage> collectOperatorCoverage(IPhysicalPlan plan, StatementContext stmtContext, Map<Integer, Location> nodeLocations)
    {
        List<OperatorCoverage> result = new ArrayList<>();
        collectOperators(plan, stmtContext, nodeLocations, result, -1);
        // Sort ascending by source line so the report mirrors SQL top-to-bottom order.
        // Operators without location info (line <= 0) sort to the end.
        result.sort(Comparator.comparingInt(op -> op.location != null
                && op.location.line() > 0 ? op.location.line()
                        : Integer.MAX_VALUE));
        return result;
    }

    private static void collectOperators(IPhysicalPlan plan, StatementContext stmtContext, Map<Integer, Location> nodeLocations, List<OperatorCoverage> result, int parentNodeId)
    {
        if (plan instanceof AnalyzeInterceptor ai)
        {
            int aiNodeId = ai.getActualNodeId();
            NodeData data = stmtContext.getNodeData(aiNodeId);
            Location loc = nodeLocations.getOrDefault(aiNodeId, Location.EMPTY);
            int execCount = data != null ? data.getExecutionCount()
                    : 0;
            long rowsOut = data != null ? data.getRowCount()
                    : 0;
            boolean covered = execCount > 0;
            result.add(new OperatorCoverage(aiNodeId, parentNodeId, ai.getName(), execCount, rowsOut, covered, loc));
            // Children use this node's ID as their parent — transparent non-AI wrappers pass the ID through
            collectOperators(ai.getInput(), stmtContext, nodeLocations, result, aiNodeId);
        }
        else
        {
            for (IPhysicalPlan child : plan.getChildren())
            {
                // Non-AI nodes are transparent: children stay under the same parent
                collectOperators(child, stmtContext, nodeLocations, result, parentNodeId);
            }
        }
    }

    private static List<BranchCoverage> collectBranchCoverage(StatementContext stmtContext)
    {
        List<BranchCoverage> result = new ArrayList<>();
        for (Map.Entry<Integer, ? extends NodeData> entry : stmtContext.getNodeData()
                .entrySet())
        {
            if (entry.getValue() instanceof CaseCoverageData cd)
            {
                result.add(new BranchCoverage(entry.getKey(), cd.getExpressionText(), cd.getWhenHits(), cd.getElseHits(), cd.getLocation()));
            }
        }
        return result;
    }

    private static List<ConditionCoverage> collectConditionCoverage(StatementContext stmtContext)
    {
        List<ConditionCoverage> result = new ArrayList<>();
        for (Map.Entry<Integer, ? extends NodeData> entry : stmtContext.getNodeData()
                .entrySet())
        {
            if (entry.getValue() instanceof LogicalBinaryCoverageData ld)
            {
                result.add(new ConditionCoverage(entry.getKey(), ld.getExpressionText(), ld.getTrueHits(), ld.getFalseHits(), ld.getLocation()));
            }
        }
        return result;
    }
}
