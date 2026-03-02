package se.kuseman.payloadbuilder.core.physicalplan;

/** Visitor definition for {@link IPhysicalPlan}'s */
public interface IPhysicalPlanVisitor<T, C>
{
    //@formatter:off
    T visit(Projection plan, C context);
    T visit(Sort plan, C context);
    T visit(Filter plan, C context);
    T visit(HashAggregate plan, C context);
    T visit(TableScan plan, C context);
    T visit(IndexSeek plan, C context);
    T visit(TableFunctionScan plan, C context);
    T visit(ExpressionScan plan, C context);
    T visit(NestedLoop plan, C context);
    T visit(HashMatch plan, C context);
    T visit(OperatorFunctionScan plan, C context);
    T visit(ConstantScan plan, C context);
    T visit(Limit plan, C context);
    T visit(Assert plan, C context);
    T visit(Concatenation plan, C context);
    T visit(AnalyzeInterceptor plan, C context);
    T visit(AssignmentPlan plan, C context);
    T visit(CachePlan plan, C context);
    T visit(DescribePlan plan, C context);
    T visit(InsertInto plan, C context);
    //@formatter:on
}
