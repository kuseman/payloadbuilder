package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Operator function object. Creates an object from provided arguments.
 */
class OperatorObjectFunction extends OperatorFunctionInfo
{
    OperatorObjectFunction()
    {
        super("object");
    }

    @Override
    public ResolvedType getType(Schema input)
    {
        return ObjectFunctionImpl.getOperatorType(input);
    }

    @Override
    public ValueVector eval(IExecutionContext context, String catalogAlias, TupleVector input)
    {
        return ObjectFunctionImpl.evalOperator(input, context);
    }
}
