package se.kuseman.payloadbuilder.core.physicalplan;

/** Base class for logical visitors */
public abstract class APhysicalPlanVisitor<T, C> implements IPhysicalPlanVisitor<T, C>
{
    protected T defaultResult(C context)
    {
        return null;
    }

    protected T aggregate(T result, T nextResult)
    {
        return nextResult;
    }

    protected T visitChildren(C context, IPhysicalPlan plan)
    {
        T result = defaultResult(context);
        for (IPhysicalPlan p : plan.getChildren())
        {
            T value = p.accept(this, context);
            result = aggregate(result, value);
        }
        return result;
    }

    @Override
    public T visit(Projection plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Sort plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Filter plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(HashAggregate plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(TableScan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(TableFunctionScan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(ExpressionScan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(NestedLoop plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(HashMatch plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(OperatorFunctionScan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(ConstantScan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Limit plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Assert plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Concatenation plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(IndexSeek plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(AnalyzeInterceptor plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(AssignmentPlan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(CachePlan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(DescribePlan plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(InsertInto plan, C context)
    {
        return visitChildren(context, plan);
    }
}
