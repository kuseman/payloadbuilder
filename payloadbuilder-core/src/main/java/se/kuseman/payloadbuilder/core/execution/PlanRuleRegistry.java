package se.kuseman.payloadbuilder.core.execution;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-local registry that connects the plan rule engine to rules registered by test extensions (e.g. {@code QueryCoverageExtension}). Rules are registered per thread so parallel test execution
 * works without interference.
 */
public class PlanRuleRegistry
{
    private static final ThreadLocal<List<PlanRule>> RULES = ThreadLocal.withInitial(ArrayList::new);

    private PlanRuleRegistry()
    {
    }

    /** Registers a rule that will be checked against every compiled physical plan on this thread. */
    public static void register(PlanRule rule)
    {
        RULES.get()
                .add(rule);
    }

    /** Deregisters a previously registered rule. */
    public static void deregister(PlanRule rule)
    {
        RULES.get()
                .remove(rule);
    }

    /** Returns {@code true} if at least one rule is registered on the current thread. */
    public static boolean isActive()
    {
        return !RULES.get()
                .isEmpty();
    }

    /** Invokes all registered rules with the given operator info. Called by the plan-rule walk in {@link se.kuseman.payloadbuilder.core.planning.StatementRewriter}. */
    public static void check(PlanOperatorInfo operator)
    {
        for (PlanRule rule : RULES.get())
        {
            rule.check(operator);
        }
    }
}
