package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
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
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        return new ObjectArrayAggregator(arguments);
    }

    private static class ObjectArrayAggregator implements IAggregator
    {
        private final List<IExpression> arguments;
        private ObjectList<List<IValueVectorBuilder>> groupBuilders;

        ObjectArrayAggregator(List<IExpression> arguments)
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
                else
                {
                    List<IValueVectorBuilder> builders = groupBuilders.get(groupId);
                    if (builders == null)
                    {
                        builders = new ArrayList<>(Collections.nCopies(size, null));
                        groupBuilders.set(groupId, builders);
                    }

                    for (int j = 0; j < size; j++)
                    {
                        ValueVector v = arguments.get(j)
                                .eval(group, context);

                        IValueVectorBuilder builder = builders.get(j);
                        if (builder == null)
                        {
                            builder = context.getVectorBuilderFactory()
                                    .getValueVectorBuilder(v.type(), v.size());
                            builders.set(j, builder);
                        }

                        builder.copy(v);
                    }
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            ResolvedType aggregateType = ObjectArrayFunctionImpl.getAggregateType(arguments);
            Schema schema = aggregateType.getSchema();

            int size = groupBuilders.size();
            IObjectVectorBuilder objectVectorBuilder = context.getVectorBuilderFactory()
                    .getObjectVectorBuilder(aggregateType, size);
            for (int i = 0; i < size; i++)
            {
                List<IValueVectorBuilder> builders = groupBuilders.get(i);
                if (builders == null)
                {
                    objectVectorBuilder.putNull();
                }
                else
                {
                    int bsize = builders.size();
                    List<ValueVector> vectors = new ArrayList<>(bsize);
                    for (int j = 0; j < bsize; j++)
                    {
                        vectors.add(builders.get(j)
                                .build());
                    }
                    objectVectorBuilder.put(TupleVector.of(schema, vectors));
                }
            }

            return objectVectorBuilder.build();
        }
    }
}
