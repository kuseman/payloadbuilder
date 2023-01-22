package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector.OutputWritable;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Aggregation that transforms input into an object/array/object-array structure. Result is an {@link Type#OutputWritable} that can be lazily written with an {@link OutputWritable}
 */
public class AggregateStructureValueFunction extends ScalarFunctionInfo
{
    private final AStructureValueFunction function;

    public AggregateStructureValueFunction(Catalog catalog, AStructureValueFunction function)
    {
        super(catalog, requireNonNull(function, "function").getOutputType()
                .name()
                .toLowerCase(), FunctionType.AGGREGATE);
        this.function = function;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.OutputWritable);
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<? extends IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }
        return function.eval(groups, arguments, context);
    }
}