package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Function object. Creates an object of provided arguments.
 */
class ObjectFunction extends ScalarFunctionInfo
{
    ObjectFunction()
    {
        super("object", FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ObjectFunctionImpl.getScalarType(arguments);
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return ObjectFunctionImpl.getAggregateType(arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return ObjectFunctionImpl.evalScalar(input, arguments, context);
    }

    @Override
    public IExpression fold(IExecutionContext context, List<IExpression> arguments)
    {
        if (arguments.isEmpty())
        {
            return context.getExpressionFactory()
                    .createObjectExpression(ObjectVector.EMPTY);
        }
        else if (arguments.stream()
                .allMatch(IExpression::isConstant))
        {
            ResolvedType type = ObjectFunctionImpl.getScalarType(arguments);
            Schema schema = type.getSchema();

            int size = arguments.size();
            List<ValueVector> vectors = new ArrayList<>(size / 2);
            for (int i = 0; i < size; i += 2)
            {
                vectors.add(arguments.get(i + 1)
                        .eval(context));
            }

            return context.getExpressionFactory()
                    .createObjectExpression(ObjectVector.wrap(TupleVector.of(schema, vectors)));
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

        return ObjectFunctionImpl.evalAggregate(groups, arguments, context);
    }
}
