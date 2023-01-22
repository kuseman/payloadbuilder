package se.kuseman.payloadbuilder.api.catalog;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Definition of a scalar function */
// CSOFF
public abstract class ScalarFunctionInfo extends FunctionInfo
// CSON
{
    public ScalarFunctionInfo(Catalog catalog, String name, FunctionType type)
    {
        super(catalog, name, type);
    }

    /**
     * Data type of this function
     *
     * @param arguments Supplier for arguments data types
     */
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Any);
    }

    /** Evaluate this function in scalar mode. */
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        throw new IllegalArgumentException("Scalar not implemented. eval: " + getClass().getSimpleName());
    }

    /**
     * Evaluate this function in scalar mode with an aggregation mode. This is typically an aggregate function that can also act as a scalar function.
     * 
     * <pre>
     * Example function sum
     * 
     * select sum(DISTINCT col1)                                <--- aggregate function with aggregate mode
     * from table
     * group by col2
     * 
     * select collection.map(x -> x.col > 10).sum(DISTINCT)     <--- scalar function with aggregate mode
     * from .....
     * 
     * </pre>
     */
    public ValueVector evalScalar(IExecutionContext context, AggregateMode mode, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        throw new IllegalArgumentException("Scalar with aggregate mode not implemented. eval: " + getClass().getSimpleName());
    }

    /** Evaluate this function in aggregate mode. Used when this function is an aggregate function */
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<? extends IExpression> arguments)
    {
        throw new IllegalArgumentException("Aggregate not implemented. eval: " + getClass().getSimpleName());
    }

    /**
     * Returns true if this function supports code generation
     *
     * @param arguments Function arguments
     */
    public boolean isCodeGenSupported(List<? extends IExpression> arguments)
    {
        return false;
    }

    /** Returns true if this function is contant */
    public boolean isConstant(List<? extends IExpression> arguments)
    {
        return arguments.stream()
                .allMatch(e -> e.isConstant());
    }

    /** Mode of aggregation */
    public enum AggregateMode
    {
        ALL,
        DISTINCT
    }
}
