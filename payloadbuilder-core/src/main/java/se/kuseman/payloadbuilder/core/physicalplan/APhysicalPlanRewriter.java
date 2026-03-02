package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.List;

/** Base class for rewriters of {@link IPhysicalPlan}'s */
public abstract class APhysicalPlanRewriter<C> extends APhysicalPlanVisitor<IPhysicalPlan, C>
{
    protected IPhysicalPlan visit(IPhysicalPlan plan, C context)
    {
        return plan.accept(this, context);
    }

    @Override
    public IPhysicalPlan visit(Projection plan, C context)
    {
        return new Projection(plan.getNodeId(), visit(plan.getInput(), context), plan.getSchema(), plan.getExpressions(), plan.getParentTableSource());
    }

    @Override
    public IPhysicalPlan visit(Sort plan, C context)
    {
        return new Sort(plan.getNodeId(), visit(plan.getInput(), context), plan.getSortItems());
    }

    @Override
    public IPhysicalPlan visit(Filter plan, C context)
    {
        return new Filter(plan.getNodeId(), visit(plan.getInput(), context), plan.getPredicate());
    }

    @Override
    public IPhysicalPlan visit(HashAggregate plan, C context)
    {
        return new HashAggregate(plan.getNodeId(), visit(plan.getInput(), context), plan.getAggregateExpressions(), plan.getProjectionExpressions(), plan.getParentTableSource());
    }

    @Override
    public IPhysicalPlan visit(TableScan plan, C context)
    {
        // Nothing to rewrite
        return plan;
    }

    @Override
    public IPhysicalPlan visit(IndexSeek plan, C context)
    {
        // Nothing to rewrite
        return plan;
    }

    @Override
    public IPhysicalPlan visit(TableFunctionScan plan, C context)
    {
        // Nothing to rewrite
        return plan;
    }

    @Override
    public IPhysicalPlan visit(ExpressionScan plan, C context)
    {
        // Nothing to rewrite
        return plan;
    }

    @Override
    public IPhysicalPlan visit(NestedLoop plan, C context)
    {
        return NestedLoop.copy(plan, visit(plan.getOuter(), context), visit(plan.getInner(), context));
    }

    @Override
    public IPhysicalPlan visit(HashMatch plan, C context)
    {
        return new HashMatch(plan, visit(plan.getOuter(), context), visit(plan.getInner(), context));
    }

    @Override
    public IPhysicalPlan visit(OperatorFunctionScan plan, C context)
    {
        return new OperatorFunctionScan(plan.getNodeId(), visit(plan.getInput(), context), plan.getFunction(), plan.getCatalogAlias(), plan.getSchema());
    }

    @Override
    public IPhysicalPlan visit(ConstantScan plan, C context)
    {
        // Noting to rewrite
        return plan;
    }

    @Override
    public IPhysicalPlan visit(Limit plan, C context)
    {
        return new Limit(plan.getNodeId(), visit(plan.getInput(), context), plan.getLimitExpression());
    }

    @Override
    public IPhysicalPlan visit(Assert plan, C context)
    {
        return Assert.copy(plan, visit(plan.getInput(), context));
    }

    @Override
    public IPhysicalPlan visit(Concatenation plan, C context)
    {
        List<IPhysicalPlan> inputs = plan.getInputs()
                .stream()
                .map(p -> visit(p, context))
                .toList();
        return new Concatenation(plan.getNodeId(), plan.getSchema(), inputs);
    }

    @Override
    public IPhysicalPlan visit(AnalyzeInterceptor plan, C context)
    {
        return new AnalyzeInterceptor(plan.getNodeId(), visit(plan.getInput(), context));
    }

    @Override
    public IPhysicalPlan visit(AssignmentPlan plan, C context)
    {
        return new AssignmentPlan(plan.getNodeId(), visit(plan.getInput(), context));
    }

    @Override
    public IPhysicalPlan visit(CachePlan plan, C context)
    {
        return new CachePlan(plan.getNodeId(), visit(plan.getInput(), context));
    }

    @Override
    public IPhysicalPlan visit(DescribePlan plan, C context)
    {
        return new DescribePlan(plan.getNodeId(), visit(plan.getInput(), context), plan.isAnalyze(), plan.getAnalyzeFormat(), plan.getQueryText());
    }

    @Override
    public IPhysicalPlan visit(InsertInto plan, C context)
    {
        return new InsertInto(plan.getNodeId(), visit(plan.getInput(), context), plan.getInsertColumns(), plan.getDatasink());
    }
}
