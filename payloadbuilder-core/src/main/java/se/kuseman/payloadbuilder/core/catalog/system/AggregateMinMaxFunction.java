package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.function.IntFunction;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
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
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }
        final IExpression arg = arguments.get(0);
        return aggregate(context, groups.size(), getAggregateType(arguments), group ->
        {
            TupleVector groupVector = groups.getTable(group);
            return arg.eval(groupVector, context);
        });
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

        return aggregate(context, input.getRowCount(), getType(arguments), group -> value.getArray(group));
    }

    private ValueVector aggregate(IExecutionContext context, int size, ResolvedType type, IntFunction<ValueVector> vectorSupplier)
    {
        // Result is array with row index for each group with the min/max index
        int[] result = new int[size];
        ValueVector[] vectors = new ValueVector[size];

        // Find out min/max for each group
        for (int i = 0; i < size; i++)
        {
            ValueVector vector = vectorSupplier.apply(i);
            vectors[i] = vector;

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
                int c = VectorUtils.compare(vector, vector, type.getType(), currentIndex, j);

                // Switch index
                if ((!min
                        && c < 0)
                        || (min
                                && c > 0))
                {
                    currentIndex = j;
                }
            }

            result[i] = prevNull ? -1
                    : currentIndex;
        }

        IValueVectorBuilder builder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(type, size);
        for (int i = 0; i < size; i++)
        {
            if (result[i] == -1)
            {
                builder.putNull();
            }
            else
            {
                builder.put(vectors[i], result[i]);
            }
        }

        return builder.build();
    }

    @Override
    public String toString()
    {
        return min ? "MIN"
                : "MAX";
    }
}
