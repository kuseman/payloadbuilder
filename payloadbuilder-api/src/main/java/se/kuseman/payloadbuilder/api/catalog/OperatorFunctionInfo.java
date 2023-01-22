package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Base class for operator functions */
public abstract class OperatorFunctionInfo extends FunctionInfo
{
    public OperatorFunctionInfo(Catalog catalog, String name)
    {
        super(catalog, name, FunctionType.OPERATOR);
    }

    /** Evaluate this function against input vector. */
    public abstract ValueVector eval(IExecutionContext context, String catalogAlias, TupleVector input);

    /** Return the return type that this function will produce */
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Any);
    }
}
