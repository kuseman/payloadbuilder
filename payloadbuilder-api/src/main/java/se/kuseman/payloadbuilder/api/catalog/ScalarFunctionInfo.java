package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Arrays.asList;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Definition of a scalar function */
// CSOFF
public abstract class ScalarFunctionInfo extends FunctionInfo
// CSON
{
    public ScalarFunctionInfo(String name, FunctionType type)
    {
        super(name, validate(type));
    }

    private static FunctionType validate(FunctionType type)
    {
        if (!(type == FunctionType.AGGREGATE
                || type == FunctionType.SCALAR
                || type == FunctionType.SCALAR_AGGREGATE))
        {
            throw new IllegalArgumentException("Function type must be one of: " + asList(FunctionType.AGGREGATE, FunctionType.SCALAR, FunctionType.SCALAR_AGGREGATE));
        }
        return type;
    }

    /**
     * Folds this function and returns a new expression. This can be used to return a literal expression if all arguments are constant etc.
     *
     * @return New expression if one could be created otherwise null
     */
    public IExpression fold(IExecutionContext context, List<IExpression> arguments)
    {
        return null;
    }

    /**
     * Data type of this function
     *
     * @param arguments Supplier for arguments data types
     */
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Any);
    }

    /**
     * Data type of this function in aggregate mode. NOTE! Only applicable if {@link FunctionType} is {@link FunctionType#AGGREGATE} or {@link FunctionType#SCALAR_AGGREGATE}
     *
     * @param arguments Supplier for arguments data types
     */
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Any);
    }

    /** Evaluate this function in scalar mode. */
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        throw new IllegalArgumentException("Scalar not implemented. eval: " + getClass().getSimpleName());
    }

    /**
     * <pre>
     * Evaluate this function in scalar mode with an aggregation mode.
     * This is typically an aggregate function that can also act as a scalar function.
     *
     * Example function sum
     * 
     * select sum(DISTINCT col1)                                &lt;--- aggregate function with aggregate mode
     * from table
     * group by col2
     * 
     * select collection.map(x -&gt; x.col &gt; 10).sum(DISTINCT)     &lt;--- scalar function with aggregate mode
     * from .....
     * </pre>
     */
    public ValueVector evalScalar(IExecutionContext context, AggregateMode mode, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        throw new IllegalArgumentException("Scalar with aggregate mode not implemented. eval: " + getClass().getSimpleName());
    }

    /** Evaluate this function in aggregate mode. Used when this function is an aggregate function */
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        throw new IllegalArgumentException("Aggregate not implemented. eval: " + getClass().getSimpleName());
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
