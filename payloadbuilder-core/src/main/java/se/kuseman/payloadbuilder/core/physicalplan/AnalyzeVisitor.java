package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.execution.QuerySession;

/** Visitor that creates a analyze plan from input plan. */
public class AnalyzeVisitor extends APhysicalPlanRewriter<AnalyzeVisitor.Context>
{
    private static final AnalyzeVisitor INSTANCE = new AnalyzeVisitor();

    static class Context
    {
        int nodeId;
    }

    @Override
    protected IPhysicalPlan visit(IPhysicalPlan plan, Context context)
    {
        // Make sure we don't produce analyze plans of already analyzed plans
        if (plan instanceof AnalyzeInterceptor)
        {
            return plan;
        }

        IPhysicalPlan analyzePlan = super.visit(plan, context);
        return new AnalyzeInterceptor(context.nodeId++, analyzePlan);
    }

    /** Describe provided plan with analyze type. */
    public static IPhysicalPlan describe(IPhysicalPlan plan, AnlayzeType analyzeType, AnalyzeFormat analyzeFormat, String queryText)
    {
        if (plan instanceof DescribePlan)
        {
            return plan;
        }

        boolean isAnalyze = analyzeType == AnlayzeType.ANALYZE;
        int nodeId = -1;
        if (isAnalyze)
        {
            plan = analyze(plan);
            nodeId = plan.getActualNodeId() + 1;
        }
        else
        {
            nodeId = getMaxNodeId(plan);
        }

        return new DescribePlan(nodeId, plan, isAnalyze, analyzeFormat, queryText);
    }

    /** Create analyze plan from input plan. */
    private static IPhysicalPlan analyze(IPhysicalPlan plan)
    {
        int maxNodeId = getMaxNodeId(plan);
        Context ctx = new Context();
        ctx.nodeId = maxNodeId;
        return INSTANCE.visit(plan, ctx);
    }

    private static int getMaxNodeId(IPhysicalPlan plan)
    {
        int maxNodeId = -1;
        List<IPhysicalPlan> queue = new ArrayList<>();
        queue.add(plan);
        while (!queue.isEmpty())
        {
            IPhysicalPlan current = queue.remove(0);
            maxNodeId = Math.max(maxNodeId, current.getNodeId());
            queue.addAll(current.getChildren());
        }
        maxNodeId++;
        return maxNodeId;
    }

    /** Data for performing an analzye of a query statement. */
    public record AnalyzeData(AnlayzeType type, AnalyzeFormat format, boolean extendOutput)
    {
        /** Extracts analyze data from a session instance. */
        public static AnalyzeData fromSession(QuerySession session)
        {
            return extract(null, session);
        }

        /** Extracts analyze data from a session instance. */
        private static AnalyzeData extract(AnlayzeType type, QuerySession session)
        {
            boolean extendedOutput = true;
            ValueVector prop = session.getSystemProperty(QuerySession.PLAN_EXTENDED_OUTPUT);
            if (!prop.isNull(0))
            {
                extendedOutput = prop.getBoolean(0);
            }

            if (type == null)
            {
                type = AnlayzeType.NONE;
                prop = session.getSystemProperty(QuerySession.PLAN_ANALYZE);
                if (prop.getPredicateBoolean(0))
                {
                    type = AnlayzeType.ANALYZE;
                }
                if (type == AnlayzeType.NONE)
                {
                    prop = session.getSystemProperty(QuerySession.PLAN_DESCRIBE);
                    if (prop.getPredicateBoolean(0))
                    {
                        type = AnlayzeType.DESCRIBE;
                    }
                }
            }

            AnalyzeFormat format = AnalyzeFormat.TABLE;
            prop = session.getSystemProperty(QuerySession.PLAN_FORMAT);
            if (!prop.isNull(0))
            {
                format = AnalyzeFormat.valueOf(prop.getString(0)
                        .toString()
                        .toUpperCase());
            }

            return new AnalyzeData(type, format, extendedOutput);
        }
    }

    /** Type of analyze. */
    public enum AnlayzeType
    {
        NONE,
        /** Describe plan without execution. */
        DESCRIBE,
        /** Describe plan with execution stats. */
        ANALYZE,
    }

    /** Output format of analyze. */
    public enum AnalyzeFormat
    {
        /** JSON tree of plan. Produces a result set with one row and one column. */
        JSON,
        /** Regular table vector. */
        TABLE
    }
}
