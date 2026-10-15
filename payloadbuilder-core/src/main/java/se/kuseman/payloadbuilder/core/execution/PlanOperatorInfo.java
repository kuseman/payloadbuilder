package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;

/**
 * Structured information about a single operator node in a compiled physical plan, passed to {@link PlanRule#check(PlanOperatorInfo)} during compilation. The underlying {@link IPhysicalPlan} is
 * exposed directly so rules can use {@code instanceof} pattern matching to access type-specific properties.
 *
 * <pre>
 * op -&gt;
 * {
 *     if (op.plan() instanceof TableScan ts
 *             &amp;&amp; ts.getCatalogAlias() != null
 *             &amp;&amp; !ts.getCatalogAlias()
 *                     .startsWith("#"))
 *     {
 *         throw new PlanRuleViolationException("Full scan on catalog '" + ts.getCatalogAlias() + "' in '" + op.queryName() + "' line " + op.sourceLine());
 *     }
 * }
 * </pre>
 *
 * <p>
 * Use {@link #parent()} to walk up the ancestor chain and distinguish contexts — e.g. a scan inside a join vs a scan inside a SELECT INTO:
 * </p>
 *
 * <pre>
 * op -&gt;
 * {
 *     if (op.plan() instanceof IndexSeek
 *             &amp;&amp; op.hasAncestor(NestedLoop.class))
 *     {
 *         // scan is the inner side of a join
 *     }
 * }
 * </pre>
 *
 * @param plan the physical plan node — use {@code instanceof} to access type-specific fields
 * @param sourceLine source line in the SQL text; {@code 0} when location information is unavailable
 * @param queryName the compile-time name passed to {@code Payloadbuilder.compile(session, sql, name)}, or {@code null} for unnamed queries
 * @param parent the parent operator info, or {@code null} if this is the root operator
 */
public record PlanOperatorInfo(IPhysicalPlan plan, int sourceLine, String queryName, PlanOperatorInfo parent)
{
    /** Convenience constructor for the root operator (no parent). */
    public PlanOperatorInfo(IPhysicalPlan plan, int sourceLine, String queryName)
    {
        this(plan, sourceLine, queryName, null);
    }

    /**
     * Returns {@code true} if any ancestor in the parent chain is an instance of the given physical plan type. Useful for distinguishing execution contexts — e.g. whether a scan is the inner side of
     * a join, or directly under a SELECT INTO.
     */
    public boolean hasAncestor(Class<? extends IPhysicalPlan> type)
    {
        PlanOperatorInfo p = parent;
        while (p != null)
        {
            if (type.isInstance(p.plan()))
            {
                return true;
            }
            p = p.parent();
        }
        return false;
    }

    /**
     * Returns the nearest ancestor whose plan is an instance of the given type, or {@code null} if none exists.
     */
    public PlanOperatorInfo firstAncestor(Class<? extends IPhysicalPlan> type)
    {
        PlanOperatorInfo p = parent;
        while (p != null)
        {
            if (type.isInstance(p.plan()))
            {
                return p;
            }
            p = p.parent();
        }
        return null;
    }
}
