package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Function array. Creates an array of provided arguments.
 */
class ArrayFunction extends ScalarFunctionInfo
{
    ArrayFunction()
    {
        super("array", FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public String getDescription()
    {
        return "Creates an array of provided arguments." + System.lineSeparator() + "ie. array(1,2, true, 'string')";
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ArrayFunctionImpl.getScalarType(arguments);
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return ArrayFunctionImpl.getAggregateType(arguments);
    }

    @Override
    public IExpression fold(IExecutionContext context, List<IExpression> arguments)
    {
        if (arguments.isEmpty())
        {
            return context.getExpressionFactory()
                    .createArrayExpression(ValueVector.empty(ResolvedType.array(Type.Any)));
        }
        else if (arguments.stream()
                .allMatch(IExpression::isConstant))
        {
            ResolvedType type = ArrayFunctionImpl.getScalarType(arguments);

            int size = arguments.size();
            IValueVectorBuilder builder = context.getVectorBuilderFactory()
                    .getValueVectorBuilder(type.getSubType(), size);

            for (IExpression arg : arguments)
            {
                builder.put(arg.eval(context), 0);
            }

            return context.getExpressionFactory()
                    .createArrayExpression(builder.build());
        }
        return null;
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        return ArrayFunctionImpl.evalAggregate(groups, arguments, context);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return ArrayFunctionImpl.evalScalar(input, arguments, context);
    }
}
