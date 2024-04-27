package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorFactory;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Min aggregate. Return the min value among values */
class AggregateMinMaxFunction extends ScalarFunctionInfo
{
    private final boolean min;

    AggregateMinMaxFunction(boolean min)
    {
        super(min ? "min"
                : "max", FunctionType.SCALAR_AGGREGATE);
        this.min = min;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        ResolvedType result = arguments.get(0)
                .getType();

        // In scalar mode and we have an array input type the result is the sub type
        if (result.getType() == Column.Type.Array)
        {
            result = result.getSubType();
        }

        return result;
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        // Aggregated result type is the same as argument
        return arguments.get(0)
                .getType();
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }
        IExpression expression = arguments.get(0);
        ResolvedType resolvedType = getAggregateType(arguments);
        return new MinMaxAggregator(min, expression, resolvedType);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, AggregateMode mode, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        return evalScalar(context, input, catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

        // A non array type already has correct min/max since each row is a single element
        if (value.type()
                .getType() != Column.Type.Array)
        {
            return value;
        }

        int rowCount = input.getRowCount();
        MinMaxAggregator aggregator = new MinMaxAggregator(min, arguments.get(0), getType(arguments));
        aggregator.minMaxRow = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.Any), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            ValueVector vector = value.getArray(i);
            aggregator.minMaxRow.setNull(i);
            aggregator.setMinMaxRow(vector, i, context.getVectorFactory());
        }

        return aggregator.combine(context);
    }

    private static class MinMaxAggregator implements IAggregator
    {
        private final boolean min;
        private final IExpression expression;
        private final ResolvedType resolvedType;

        /** List with min/max value per */
        private MutableValueVector minMaxRow;

        private MinMaxAggregator(boolean min, IExpression expression, ResolvedType resolvedType)
        {
            this.min = min;
            this.expression = expression;
            this.resolvedType = resolvedType;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            int groupCount = groupIds.size();

            if (minMaxRow == null)
            {
                minMaxRow = context.getVectorFactory()
                        .getMutableVector(ResolvedType.of(Type.Any), groupCount);
            }
            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector selection = selections.getArray(i);
                if (minMaxRow.size() <= groupId)
                {
                    minMaxRow.setNull(groupId);
                }
                if (selection.size() == 0)
                {
                    continue;
                }

                ValueVector result = expression.eval(input, selection, context);
                setMinMaxRow(result, groupId, context.getVectorFactory());
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            int size = minMaxRow.size();
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(resolvedType, size);
            for (int i = 0; i < size; i++)
            {
                ValueVector groupResult = (ValueVector) minMaxRow.valueAsObject(i);
                if (groupResult == null)
                {
                    resultVector.setNull(i);
                }
                else
                {
                    resultVector.copy(i, groupResult, 0);
                }
            }
            return resultVector;
        }

        /** Compares provided batch and sets min/max in stage */
        private void setMinMaxRow(ValueVector vector, int group, IVectorFactory vectorFactory)
        {
            int groupSize = vector.size();

            boolean prevNull = false;

            int currentIndex = 0;
            if (vector.isNull(currentIndex))
            {
                prevNull = true;
            }

            for (int j = 1; j < groupSize; j++)
            {
                // Null => skip current
                if (vector.isNull(j))
                {
                    continue;
                }
                else if (prevNull)
                {
                    prevNull = false;
                    currentIndex = j;
                    continue;
                }

                prevNull = false;
                int c = VectorUtils.compare(vector, vector, resolvedType.getType(), currentIndex, j);

                // Switch index
                if ((!min
                        && c < 0)
                        || (min
                                && c > 0))
                {
                    currentIndex = j;
                }
            }
            ValueVector prevRow = (ValueVector) minMaxRow.valueAsObject(group);
            // First non null group, copy this batch min/max row
            if (prevRow == null
                    && !prevNull)
            {
                MutableValueVector resultVector = vectorFactory.getMutableVector(vector.type(), 1);
                resultVector.copy(0, vector, currentIndex);
                minMaxRow.setAny(group, resultVector);
            }
            // Compare this batch min/mix with previous batch
            else if (!prevNull)
            {
                int c = VectorUtils.compare(prevRow, vector, resolvedType.getType(), 0, currentIndex);
                // Switch state min/max row
                if ((!min
                        && c < 0)
                        || (min
                                && c > 0))
                {
                    MutableValueVector resultVector = vectorFactory.getMutableVector(vector.type(), 1);
                    resultVector.copy(0, vector, currentIndex);
                    minMaxRow.setAny(group, resultVector);
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return min ? "MIN"
                : "MAX";
    }
}
