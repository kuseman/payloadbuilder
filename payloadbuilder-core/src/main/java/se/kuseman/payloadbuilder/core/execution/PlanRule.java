package se.kuseman.payloadbuilder.core.execution;

/**
 * Rule evaluated against every operator in a compiled physical plan. Register via {@link PlanRuleRegistry} or via {@code QueryCoverageExtension.addRule(rule)} in JUnit tests. Throw
 * {@link PlanRuleViolationException} to reject the plan and propagate a test failure to the {@code @Test} method that triggered the compile.
 */
@FunctionalInterface
public interface PlanRule
{
    /**
     * Called once for every operator node in the compiled physical plan tree. Throw {@link PlanRuleViolationException} to reject the plan.
     *
     * @param operator structured information about this operator — name, catalog alias, source line, query name
     */
    void check(PlanOperatorInfo operator);
}
