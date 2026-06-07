package se.kuseman.payloadbuilder.core.execution;

/** Thrown by a {@link PlanRule} to indicate that the compiled physical plan violates a rule. */
public class PlanRuleViolationException extends RuntimeException
{
    public PlanRuleViolationException(String message)
    {
        super(message);
    }
}
