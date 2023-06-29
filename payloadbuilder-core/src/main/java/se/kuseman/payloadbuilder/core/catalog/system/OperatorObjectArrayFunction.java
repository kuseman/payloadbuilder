package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Operator function object array. Creates an array of objects from provided arguments.
 */
class OperatorObjectArrayFunction extends OperatorFunctionInfo
{
    OperatorObjectArrayFunction()
    {
        super("object_array");
    }

    @Override
    public ResolvedType getType(Schema input)
    {
        return ObjectArrayFunctionImpl.getOperatorType(input);
    }

    @Override
    public ValueVector eval(IExecutionContext context, String catalogAlias, TupleVector input)
    {
        return ObjectArrayFunctionImpl.evalOperator(input, context);
    }
}
