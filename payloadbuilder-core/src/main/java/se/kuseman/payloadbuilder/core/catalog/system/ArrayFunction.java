package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

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
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(type.getSubType(), size);

            for (int i = 0; i < size; i++)
            {
                resultVector.copy(i, arguments.get(i)
                        .eval(context), 0);
            }

            return context.getExpressionFactory()
                    .createArrayExpression(resultVector);
        }
        return null;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return ArrayFunctionImpl.evalScalar(input, arguments, context);
    }

    @Override
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        return new ArrayAggregator(arguments);
    }

    private static class ArrayAggregator implements IAggregator
    {
        private final List<IExpression> arguments;
        private ObjectList<MutableValueVector> groupVectors;

        ArrayAggregator(List<IExpression> arguments)
        {
            this.arguments = arguments;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            int groupCount = groupIds.size();

            if (groupVectors == null)
            {
                groupVectors = new ObjectArrayList<>(groupCount + 1);
            }

            int size = arguments.size();
            ValueVector[] vectors = new ValueVector[size];

            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector selection = selections.getArray(i);
                groupVectors.size(Math.max(groupVectors.size(), groupId + 1));

                int rowCount = selection.size();
                if (rowCount == 0)
                {
                    continue;
                }

                MutableValueVector vector = groupVectors.get(groupId);
                if (vector == null)
                {
                    vector = context.getVectorFactory()
                            .getMutableVector(ResolvedType.of(Type.Any), rowCount * size);
                    groupVectors.set(groupId, vector);
                }

                for (int j = 0; j < size; j++)
                {
                    vectors[j] = arguments.get(j)
                            .eval(input, selection, context);
                }

                for (int k = 0; k < rowCount; k++)
                {
                    for (int j = 0; j < size; j++)
                    {
                        vector.setAny(vector.size(), vectors[j].valueAsObject(k));
                    }
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            int size = groupVectors.size();
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(ResolvedType.array(Type.Any), size);
            for (int i = 0; i < size; i++)
            {
                MutableValueVector vector = groupVectors.get(i);
                if (vector == null)
                {
                    resultVector.setNull(i);
                }
                else
                {
                    resultVector.setArray(i, vector);
                }
            }
            return resultVector;
        }
    }
}
