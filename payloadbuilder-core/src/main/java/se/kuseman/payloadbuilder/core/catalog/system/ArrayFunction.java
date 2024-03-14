package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
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
        private ObjectList<IObjectVectorBuilder> groupBuilders;

        ArrayAggregator(List<IExpression> arguments)
        {
            this.arguments = arguments;
        }

        @Override
        public void appendGroup(TupleVector groupData, IExecutionContext context)
        {
            ValueVector groupTables = groupData.getColumn(0);
            ValueVector groupIds = groupData.getColumn(1);

            int groupCount = groupData.getRowCount();

            if (groupBuilders == null)
            {
                groupBuilders = new ObjectArrayList<>(groupCount + 1);
            }

            int size = arguments.size();
            ValueVector[] vectors = new ValueVector[size];

            for (int i = 0; i < groupCount; i++)
            {
                TupleVector group = groupTables.getTable(i);
                int groupId = groupIds.getInt(i);
                groupBuilders.size(Math.max(groupBuilders.size(), groupId + 1));

                int rowCount = group.getRowCount();
                if (rowCount == 0)
                {
                    continue;
                }

                IObjectVectorBuilder builder = groupBuilders.get(groupId);
                if (builder == null)
                {
                    builder = context.getVectorBuilderFactory()
                            .getObjectVectorBuilder(ResolvedType.of(Type.Any), rowCount * size);
                    groupBuilders.set(groupId, builder);
                }

                for (int j = 0; j < size; j++)
                {
                    vectors[j] = arguments.get(j)
                            .eval(group, context);
                }

                for (int k = 0; k < rowCount; k++)
                {
                    for (int j = 0; j < size; j++)
                    {
                        builder.put(vectors[j].valueAsObject(k));
                    }
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            int size = groupBuilders.size();
            IObjectVectorBuilder objectVectorBuilder = context.getVectorBuilderFactory()
                    .getObjectVectorBuilder(ResolvedType.array(Type.Any), size);
            for (int i = 0; i < size; i++)
            {
                IObjectVectorBuilder builder = groupBuilders.get(i);
                if (builder == null)
                {
                    objectVectorBuilder.putNull();
                }
                else
                {
                    objectVectorBuilder.put(builder.build());
                }
            }
            return objectVectorBuilder.build();
        }
    }
}
