package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector.OutputWritable;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/**
 * Operator function that transforms input into an object/array/object-array structure. Result is an {@link Type#OutputWritable} that can be lazily written with an {@link OutputWritable}
 */
public class OperatorStructureValueFunction extends OperatorFunctionInfo
{
    private final AStructureValueFunction function;

    public OperatorStructureValueFunction(Catalog catalog, AStructureValueFunction function)
    {
        super(catalog, requireNonNull(function, "function").getOutputType()
                .name()
                .toLowerCase());
        this.function = function;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.OutputWritable);
    }

    @Override
    public ValueVector eval(IExecutionContext context, String catalogAlias, TupleVector input)
    {
        return function.eval(input, context);
    }
}