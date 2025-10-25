package se.kuseman.payloadbuilder.core.statement;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

/** Statement used for DESCRIBE/ANALZYE operations */
public class DescribeSelectStatement extends Statement
{
    private final Statement statement;
    private final boolean includeLogicalPlan;
    private final boolean analyze;

    public DescribeSelectStatement(Statement statement, boolean analyze, boolean includeLogicalPlan)
    {
        this.statement = requireNonNull(statement, "statement");
        this.analyze = analyze;
        this.includeLogicalPlan = includeLogicalPlan;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isIncludeLogicalPlan()
    {
        return includeLogicalPlan;
    }

    public boolean isAnalyze()
    {
        return analyze;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public List<Statement> getChildren()
    {
        return singletonList(statement);
    }
}
