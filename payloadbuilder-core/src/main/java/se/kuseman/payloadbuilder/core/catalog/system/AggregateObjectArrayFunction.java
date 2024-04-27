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
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
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
        private ObjectList<List<MutableValueVector>> groupVectors;

        ObjectArrayAggregator(List<IExpression> arguments)
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
                else
                {
                    List<MutableValueVector> vectors = groupVectors.get(groupId);
                    if (vectors == null)
                    {
                        vectors = new ArrayList<>(Collections.nCopies(size, null));
                        groupVectors.set(groupId, vectors);
                    }

                    for (int j = 0; j < size; j++)
                    {
                        ValueVector v = arguments.get(j)
                                .eval(input, selection, context);

                        MutableValueVector vector = vectors.get(j);
                        if (vector == null)
                        {
                            vector = context.getVectorFactory()
                                    .getMutableVector(v.type(), v.size());
                            vectors.set(j, vector);
                        }

                        vector.copy(vector.size(), v);
                    }
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            ResolvedType aggregateType = ObjectArrayFunctionImpl.getAggregateType(arguments);
            Schema schema = aggregateType.getSchema();

            int size = groupVectors.size();
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(aggregateType, size);
            for (int i = 0; i < size; i++)
            {
                List<MutableValueVector> vectors = groupVectors.get(i);
                if (vectors == null)
                {
                    resultVector.setNull(i);
                }
                else
                {
                    resultVector.setTable(i, TupleVector.of(schema, vectors));
                }
            }

            return resultVector;
        }
    }
}
