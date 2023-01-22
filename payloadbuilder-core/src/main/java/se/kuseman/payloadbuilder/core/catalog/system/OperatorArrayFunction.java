package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Operator function array. Creates an array of objects from provided input.
 */
class OperatorArrayFunction extends OperatorFunctionInfo
{
    OperatorArrayFunction()
    {
        super("array");
    }

    @Override
    public ResolvedType getType(Schema input)
    {
        return ArrayFunctionImpl.getOperatorType(input);
    }

    @Override
    public ValueVector eval(IExecutionContext context, String catalogAlias, TupleVector input)
    {
        return ArrayFunctionImpl.evalOperator(input, context);
    }
}
