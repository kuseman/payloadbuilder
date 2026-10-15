package se.kuseman.payloadbuilder.test;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;

import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.BranchCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.ConditionCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.OperatorCoverage;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Converts a {@link CoverageAccumulator} into a stream of JUnit 5 {@code DynamicNode}s — one container per unique query. Operators are displayed as a nested tree mirroring the physical plan
 * hierarchy: operators that wrap inner plans become {@code DynamicContainer}s so a green outer operator containing a red inner operator is immediately visible. An outer operator that ran but has at
 * least one uncovered descendant also fails its own {@code execution} test with an explicit message, combining option B (propagate failure) and option C (hierarchy). CASE/IF branches and AND/OR
 * conditions remain flat beneath the operator tree. Call from a {@code @TestFactory @Order(Integer.MAX_VALUE)} method so all regular tests have run first:
 *
 * <pre>
 * {@code @TestFactory}
 * {@code @Order(Integer.MAX_VALUE)}
 * Stream&lt;DynamicNode&gt; queryCoverageResults() {
 *     return CoverageTestFactory.from(coverage.getAccumulator());
 * }
 * </pre>
 */
public class CoverageTestFactory
{
    private CoverageTestFactory()
    {
    }

    /** Builds the dynamic test tree from the given accumulator. Returns an empty stream if no queries were collected. */
    public static Stream<DynamicNode> from(CoverageAccumulator accumulator)
    {
        if (accumulator == null
                || accumulator.getCollected()
                        .isEmpty())
        {
            return Stream.empty();
        }

        List<DynamicNode> queryContainers = new ArrayList<>();
        for (QueryCoverageData data : accumulator.getCollected())
        {
            List<DynamicNode> tests = new ArrayList<>();

            // Build parent→children index from the flat operator list
            Map<Integer, List<OperatorCoverage>> childrenByParent = new HashMap<>();
            for (OperatorCoverage op : data.getOperators())
            {
                childrenByParent.computeIfAbsent(op.parentNodeId, k -> new ArrayList<>())
                        .add(op);
            }

            // Root operators (parentNodeId == -1) seed the tree
            for (OperatorCoverage root : childrenByParent.getOrDefault(-1, Collections.emptyList()))
            {
                tests.add(buildOperatorNode(root, childrenByParent));
            }

            for (BranchCoverage br : data.getBranches())
            {
                for (int i = 0; i < br.whenHits.length; i++)
                {
                    final int branchIndex = i;
                    final long hits = br.whenHits[i];
                    tests.add(dynamicTest(branchLabel(br, "WHEN " + i, hits), () ->
                    {
                        if (hits == 0)
                        {
                            fail("WHEN " + branchIndex + " never matched — 0 hits");
                        }
                    }));
                }

                final long elseHits = br.elseHits;
                tests.add(dynamicTest(branchLabel(br, "ELSE", elseHits), () ->
                {
                    if (elseHits == 0)
                    {
                        fail("ELSE branch never taken — 0 hits");
                    }
                }));
            }

            for (ConditionCoverage cond : data.getConditions())
            {
                final long trueHits = cond.trueHits;
                final long falseHits = cond.falseHits;
                tests.add(dynamicTest(conditionLabel(cond, "true", trueHits), () ->
                {
                    if (trueHits == 0)
                    {
                        fail("condition never true — 0 hits");
                    }
                }));
                tests.add(dynamicTest(conditionLabel(cond, "false", falseHits), () ->
                {
                    if (falseHits == 0)
                    {
                        fail("condition never false — 0 hits");
                    }
                }));
            }

            if (!tests.isEmpty())
            {
                queryContainers.add(dynamicContainer(containerLabel(data), tests.stream()));
            }
        }

        return queryContainers.stream();
    }

    private static String containerLabel(QueryCoverageData data)
    {
        long totalOps = data.getOperators()
                .size();
        long coveredOps = data.getOperators()
                .stream()
                .filter(op -> op.covered)
                .count();

        long totalBranches = 0;
        long coveredBranches = 0;
        for (BranchCoverage br : data.getBranches())
        {
            for (long hits : br.whenHits)
            {
                totalBranches++;
                if (hits > 0)
                {
                    coveredBranches++;
                }
            }
            totalBranches++;
            if (br.elseHits > 0)
            {
                coveredBranches++;
            }
        }

        long totalConds = data.getConditions()
                .size() * 2L;
        long coveredConds = data.getConditions()
                .stream()
                .mapToLong(c -> (c.trueHits > 0 ? 1
                        : 0)
                        + (c.falseHits > 0 ? 1
                                : 0))
                .sum();

        StringBuilder sb = new StringBuilder(data.getQueryKey());
        sb.append(" — ")
                .append(coveredOps)
                .append("/")
                .append(totalOps)
                .append(" operators");
        if (totalBranches > 0)
        {
            sb.append(", ")
                    .append(coveredBranches)
                    .append("/")
                    .append(totalBranches)
                    .append(" branches");
        }
        if (totalConds > 0)
        {
            sb.append(", ")
                    .append(coveredConds)
                    .append("/")
                    .append(totalConds)
                    .append(" conditions");
        }
        return sb.toString();
    }

    /**
     * Builds a {@code DynamicNode} for one operator. Leaf operators become a {@code DynamicTest}. Operators with children become a {@code DynamicContainer} whose first child is an {@code execution}
     * test (B-style: fails if the operator ran but left descendants uncovered) followed by the children's own nodes.
     */
    private static DynamicNode buildOperatorNode(OperatorCoverage op, Map<Integer, List<OperatorCoverage>> childrenByParent)
    {
        String label = operatorLabel(op);
        List<OperatorCoverage> children = childrenByParent.getOrDefault(op.nodeId, Collections.emptyList());

        if (children.isEmpty())
        {
            return dynamicTest(label, () ->
            {
                if (!op.covered)
                {
                    fail("not covered — 0 executions");
                }
            });
        }

        // Operator has inner operators — promote to a container
        List<DynamicNode> childNodes = new ArrayList<>();

        // B: the operator's own execution test also fails when descendants are uncovered
        long uncovered = countUncoveredDescendants(op.nodeId, childrenByParent);
        childNodes.add(dynamicTest("execution", () ->
        {
            if (!op.covered)
            {
                fail("not covered — 0 executions");
            }
            else if (uncovered > 0)
            {
                fail("covered (" + op.executionCount
                     + " execution"
                     + (op.executionCount != 1 ? "s"
                             : "")
                     + ") but "
                     + uncovered
                     + " inner operator"
                     + (uncovered != 1 ? "s"
                             : "")
                     + " not covered — inner path not exercised in any test");
            }
        }));

        for (OperatorCoverage child : children)
        {
            childNodes.add(buildOperatorNode(child, childrenByParent));
        }

        return dynamicContainer(label, childNodes.stream());
    }

    private static long countUncoveredDescendants(int nodeId, Map<Integer, List<OperatorCoverage>> childrenByParent)
    {
        long count = 0;
        for (OperatorCoverage child : childrenByParent.getOrDefault(nodeId, Collections.emptyList()))
        {
            if (!child.covered)
            {
                count++;
            }
            count += countUncoveredDescendants(child.nodeId, childrenByParent);
        }
        return count;
    }

    private static String operatorLabel(OperatorCoverage op)
    {
        Location loc = op.location;
        if (loc != null
                && loc.line() > 0)
        {
            return op.name + " [line " + loc.line() + "]";
        }
        return op.name + " [id:" + op.nodeId + "]";
    }

    private static String branchLabel(BranchCoverage br, String branchName, long hits)
    {
        Location loc = br.location;
        if (loc != null
                && loc.line() > 0)
        {
            return "CASE [line " + loc.line() + "] → " + branchName + " (" + hits + " hits)";
        }
        return "CASE → " + branchName + " (" + hits + " hits)";
    }

    private static String conditionLabel(ConditionCoverage cond, String outcome, long hits)
    {
        Location loc = cond.location;
        if (loc != null
                && loc.line() > 0)
        {
            return cond.expressionText + " [line " + loc.line() + "] → " + outcome + " (" + hits + " hits)";
        }
        return cond.expressionText + " → " + outcome + " (" + hits + " hits)";
    }
}
