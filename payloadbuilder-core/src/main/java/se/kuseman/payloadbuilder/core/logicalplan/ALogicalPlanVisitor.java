package se.kuseman.payloadbuilder.core.logicalplan;

/** Base class for logical visitors */
public abstract class ALogicalPlanVisitor<T, C> implements ILogicalPlanVisitor<T, C>
{
    // private final IExpressionVisitor<T, C> expressionVisitor;
    //
    // public ALogicalPlanVisitor()
    // {
    // this(null);
    // }
    //
    // public ALogicalPlanVisitor(IExpressionVisitor<T, C> expressionVisitor)
    // {
    // this.expressionVisitor = expressionVisitor;
    // }

    protected T defaultResult(C context)
    {
        return null;
    }

    protected T aggregate(T result, T nextResult)
    {
        return nextResult;
    }

    protected T visitChildren(C context, ILogicalPlan plan)
    {
        T result = defaultResult(context);
        for (ILogicalPlan p : plan.getChildren())
        {
            T value = p.accept(this, context);
            result = aggregate(result, value);
        }
        return result;
    }

    @Override
    public T visit(Projection plan, C context)
    {
        // if (expressionVisitor != null)
        // {
        // for (IExpression e : plan.getExpressions())
        // {
        // e.accept(expressionVisitor, context);
        // }
        // }
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Sort plan, C context)
    {
        // if (expressionVisitor != null)
        // {
        // for (SortItem si : plan.getSortItems())
        // {
        // e.accept(expressionVisitor, context);
        // }
        // }
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Filter plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Aggregate plan, C context)
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
    public T visit(SubQuery plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Join plan, C context)
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
    public T visit(MaxRowCountAssert plan, C context)
    {
        return visitChildren(context, plan);
    }

    @Override
    public T visit(Concatenation plan, C context)
    {
        return visitChildren(context, plan);
    }
}
