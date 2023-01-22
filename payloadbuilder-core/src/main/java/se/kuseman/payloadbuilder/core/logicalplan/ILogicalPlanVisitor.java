package se.kuseman.payloadbuilder.core.logicalplan;

/** Visitor for logical plans */
public interface ILogicalPlanVisitor<T, C>
{
    //@formatter:off
    T visit(Projection plan, C context);
    T visit(Sort plan, C context);
    T visit(Filter plan, C context);
    T visit(Aggregate plan, C context);
    T visit(TableScan plan, C context);
    T visit(TableFunctionScan plan, C context);
    T visit(SubQuery plan, C context);
    T visit(Join plan, C context);
    T visit(OperatorFunctionScan plan, C context);
    T visit(ConstantScan plan, C context);
    T visit(Limit plan, C context);
    T visit(OverScan plan, C context);
    T visit(MaxRowCountAssert plan, C context);
    T visit(Concatenation plan, C context);
    //@formatter:on
}
