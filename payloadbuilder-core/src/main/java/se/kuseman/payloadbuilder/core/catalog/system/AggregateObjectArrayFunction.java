package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Aggregate function object array. Creates an array of objects from provided arguments.
 */
class AggregateObjectArrayFunction extends ScalarFunctionInfo
{
    AggregateObjectArrayFunction()
    {
        super("object_array", FunctionType.AGGREGATE);
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return ObjectArrayFunctionImpl.getAggregateType(arguments);
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        return ObjectArrayFunctionImpl.evalAggregate(groups, arguments, context);
    }
}
