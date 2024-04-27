package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

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
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        return new ObjectAggregator(arguments);
    }

    private static class ObjectAggregator implements IAggregator
    {
        private final List<IExpression> arguments;
        private ObjectList<List<MutableValueVector>> groupVectors;

        ObjectAggregator(List<IExpression> arguments)
        {
            this.arguments = arguments;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            // TODO: only need to copy first row from input since that is what an object is

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
                        vectors = new ArrayList<>(Collections.nCopies(size / 2, null));
                        groupVectors.set(groupId, vectors);
                    }

                    int builderIndex = 0;
                    for (int j = 0; j < size; j += 2)
                    {
                        // Values are at the odd indices
                        ValueVector v = arguments.get(j + 1)
                                .eval(input, selection, context);

                        MutableValueVector vector = vectors.get(builderIndex);
                        if (vector == null)
                        {
                            vector = context.getVectorFactory()
                                    .getMutableVector(v.type(), v.size());
                            vectors.set(builderIndex, vector);
                        }

                        vector.copy(vector.size(), v);
                        builderIndex++;
                    }
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            ResolvedType aggregateType = ObjectFunctionImpl.getAggregateType(arguments);
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
                    resultVector.setObject(i, ObjectVector.wrap(TupleVector.of(schema, vectors)));
                }
            }

            return resultVector;
        }
    }
}
