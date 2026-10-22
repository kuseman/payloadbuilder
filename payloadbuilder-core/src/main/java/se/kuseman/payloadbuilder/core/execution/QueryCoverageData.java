package se.kuseman.payloadbuilder.core.execution;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Coverage data gathered for a single query execution. Contains per-operator row statistics, per-branch hit counts for CASE/IF expressions, and per-condition true/false counts for AND/OR expressions.
 * Multiple executions of the same query are aggregated via {@link #merge(QueryCoverageData)}.
 */
public class QueryCoverageData
{
    /** Stable key derived from the query name or physical plan structure. Used by the accumulator to aggregate repeated executions. */
    private final String queryKey;
    private final List<OperatorCoverage> operators;
    private final List<BranchCoverage> branches;
    private final List<ConditionCoverage> conditions;

    /** Creates coverage data with an explicit query key. */
    public QueryCoverageData(String queryKey, List<OperatorCoverage> operators, List<BranchCoverage> branches, List<ConditionCoverage> conditions)
    {
        this.queryKey = queryKey;
        this.operators = operators;
        this.branches = branches;
        this.conditions = conditions;
    }

    /** Returns the stable key identifying this query — the compile-time name if set, otherwise a plan-structure hash. */
    public String getQueryKey()
    {
        return queryKey;
    }

    /** Returns per-operator coverage entries. */
    public List<OperatorCoverage> getOperators()
    {
        return operators;
    }

    /** Returns per-branch coverage entries (CASE/IF expressions). */
    public List<BranchCoverage> getBranches()
    {
        return branches;
    }

    /** Returns per-condition coverage entries (AND/OR expressions). */
    public List<ConditionCoverage> getConditions()
    {
        return conditions;
    }

    /** Merges another execution's coverage data into this object in place. Sums all counts. */
    public void merge(QueryCoverageData other)
    {
        Map<Integer, OperatorCoverage> byId = operators.stream()
                .collect(Collectors.toMap(o -> o.nodeId, Function.identity()));
        for (OperatorCoverage otherOp : other.operators)
        {
            OperatorCoverage existing = byId.get(otherOp.nodeId);
            if (existing != null)
            {
                existing.executionCount += otherOp.executionCount;
                existing.rowsOut += otherOp.rowsOut;
                existing.covered = existing.covered
                        || otherOp.covered;
            }
        }

        Map<Integer, BranchCoverage> branchById = branches.stream()
                .collect(Collectors.toMap(b -> b.nodeId, Function.identity()));
        for (BranchCoverage otherBr : other.branches)
        {
            BranchCoverage existing = branchById.get(otherBr.nodeId);
            if (existing != null)
            {
                for (int i = 0; i < existing.whenHits.length
                        && i < otherBr.whenHits.length; i++)
                {
                    existing.whenHits[i] += otherBr.whenHits[i];
                }
                existing.elseHits += otherBr.elseHits;
            }
        }

        Map<Integer, ConditionCoverage> condById = conditions.stream()
                .collect(Collectors.toMap(c -> c.nodeId, Function.identity()));
        for (ConditionCoverage otherCond : other.conditions)
        {
            ConditionCoverage existing = condById.get(otherCond.nodeId);
            if (existing != null)
            {
                existing.trueHits += otherCond.trueHits;
                existing.falseHits += otherCond.falseHits;
            }
        }
    }

    /** Serialize coverage data to a compact JSON string. */
    public String toJson()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"operators\":{");
        boolean firstOp = true;
        for (OperatorCoverage op : operators)
        {
            if (!firstOp)
            {
                sb.append(",");
            }
            firstOp = false;
            sb.append("\"")
                    .append(op.nodeId)
                    .append("\":{");
            sb.append("\"name\":\"")
                    .append(escapeJson(op.name))
                    .append("\"");
            sb.append(",\"execution_count\":")
                    .append(op.executionCount);
            sb.append(",\"rows_out\":")
                    .append(op.rowsOut);
            sb.append(",\"covered\":")
                    .append(op.covered);
            if (op.location != null
                    && op.location.line() > 0)
            {
                sb.append(",\"location\":{\"line\":")
                        .append(op.location.line())
                        .append(",\"start\":")
                        .append(op.location.startOffset())
                        .append(",\"end\":")
                        .append(op.location.endOffset())
                        .append("}");
            }
            sb.append("}");
        }
        sb.append("}");

        if (!branches.isEmpty())
        {
            sb.append(",\"branches\":{");
            boolean firstBr = true;
            for (BranchCoverage br : branches)
            {
                if (!firstBr)
                {
                    sb.append(",");
                }
                firstBr = false;
                sb.append("\"")
                        .append(br.nodeId)
                        .append("\":{");
                sb.append("\"expression\":\"")
                        .append(escapeJson(br.expressionText))
                        .append("\"");
                sb.append(",\"when_hits\":[");
                for (int i = 0; i < br.whenHits.length; i++)
                {
                    if (i > 0)
                    {
                        sb.append(",");
                    }
                    sb.append(br.whenHits[i]);
                }
                sb.append("]");
                sb.append(",\"else_hits\":")
                        .append(br.elseHits);
                if (br.location != null
                        && br.location.line() > 0)
                {
                    sb.append(",\"location\":{\"line\":")
                            .append(br.location.line())
                            .append(",\"start\":")
                            .append(br.location.startOffset())
                            .append(",\"end\":")
                            .append(br.location.endOffset())
                            .append("}");
                }
                sb.append("}");
            }
            sb.append("}");
        }

        if (!conditions.isEmpty())
        {
            sb.append(",\"conditions\":{");
            boolean firstCond = true;
            for (ConditionCoverage cond : conditions)
            {
                if (!firstCond)
                {
                    sb.append(",");
                }
                firstCond = false;
                sb.append("\"")
                        .append(cond.nodeId)
                        .append("\":{");
                sb.append("\"expression\":\"")
                        .append(escapeJson(cond.expressionText))
                        .append("\"");
                sb.append(",\"true_hits\":")
                        .append(cond.trueHits);
                sb.append(",\"false_hits\":")
                        .append(cond.falseHits);
                if (cond.location != null
                        && cond.location.line() > 0)
                {
                    sb.append(",\"location\":{\"line\":")
                            .append(cond.location.line())
                            .append(",\"start\":")
                            .append(cond.location.startOffset())
                            .append(",\"end\":")
                            .append(cond.location.endOffset())
                            .append("}");
                }
                sb.append("}");
            }
            sb.append("}");
        }

        sb.append("}");
        return sb.toString();
    }

    private static String escapeJson(String s)
    {
        if (s == null)
        {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /** Coverage data for a single physical operator node. Fields are mutable to support merging. */
    public static class OperatorCoverage
    {
        public final int nodeId;
        /** Node ID of the enclosing operator in the physical plan tree, or {@code -1} for top-level operators. Used to build a hierarchical test tree. */
        public final int parentNodeId;
        public final String name;
        public int executionCount;
        public long rowsOut;
        public boolean covered;
        public final Location location;

        /** Creates an operator coverage entry. */
        public OperatorCoverage(int nodeId, int parentNodeId, String name, int executionCount, long rowsOut, boolean covered, Location location)
        {
            this.nodeId = nodeId;
            this.parentNodeId = parentNodeId;
            this.name = name;
            this.executionCount = executionCount;
            this.rowsOut = rowsOut;
            this.covered = covered;
            this.location = location;
        }
    }

    /** Coverage data for a single CASE/IF expression. Fields are mutable to support merging. */
    public static class BranchCoverage
    {
        public final int nodeId;
        public final String expressionText;
        public final long[] whenHits;
        public long elseHits;
        public final Location location;

        /** Creates a branch coverage entry. */
        public BranchCoverage(int nodeId, String expressionText, long[] whenHits, long elseHits, Location location)
        {
            this.nodeId = nodeId;
            this.expressionText = expressionText;
            this.whenHits = whenHits;
            this.elseHits = elseHits;
            this.location = location;
        }
    }

    /** Coverage data for a single AND/OR expression — tracks how many rows the condition evaluated to true vs. false. Fields are mutable to support merging. */
    public static class ConditionCoverage
    {
        public final int nodeId;
        public final String expressionText;
        public long trueHits;
        public long falseHits;
        public final Location location;

        /** Creates a condition coverage entry. */
        public ConditionCoverage(int nodeId, String expressionText, long trueHits, long falseHits, Location location)
        {
            this.nodeId = nodeId;
            this.expressionText = expressionText;
            this.trueHits = trueHits;
            this.falseHits = falseHits;
            this.location = location;
        }
    }
}
