package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;

/** String agg function that aggregates an array of strings with a delimiter. */
class StringAggFunction extends ScalarFunctionInfo
{
    StringAggFunction()
    {
        super("string_agg", FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return getType(arguments);
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return evalScalar(context, input, ValueVector.range(0, input.getRowCount()), catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, ValueVector selection, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, selection, context);
        ValueVector delimiter = arguments.get(1)
                .eval(input, selection, context);

        int size = selection.size();
        MutableValueVector result = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.String), size);

        List<UTF8String> strings = new ArrayList<>();

        for (int i = 0; i < size; i++)
        {
            if (value.isNull(i))
            {
                result.setNull(i);
            }
            else
            {
                Object obj = value.valueAsObject(i);
                ValueVector array = (ValueVector) VectorUtils.convertToValueVector(obj, true);
                UTF8String del = delimiter.isNull(i) ? UTF8String.EMPTY
                        : delimiter.getString(0);

                UTF8String rowValue = aggregate(strings, array, null, del);
                if (rowValue == null)
                {
                    result.setNull(i);
                }
                else
                {
                    result.setString(i, rowValue);
                }
            }
        }

        return result;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, AggregateMode mode, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return evalScalar(context, mode, input, ValueVector.range(0, input.getRowCount()), catalogAlias, arguments);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, AggregateMode mode, TupleVector input, ValueVector selection, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        return evalScalar(context, input, selection, catalogAlias, arguments);
    }

    @Override
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        if (!(arguments.get(1)
                .isConstant()
                || arguments.get(1) instanceof VariableExpression))
        {
            throw new QueryException("Delimiter to function string_agg in aggregate mode must be a constant of a variable expression");
        }
        return new Aggregator(arguments.get(0), arguments.get(1));
    }

    static class Aggregator implements IAggregator
    {
        private final IExpression value;
        private final IExpression delimiter;
        private UTF8String delimiterString;
        private MutableValueVector result;

        Aggregator(IExpression value, IExpression delimiter)
        {
            this.value = value;
            this.delimiter = delimiter;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            if (delimiterString == null)
            {
                ValueVector vv = delimiter.eval(context);
                delimiterString = vv.isNull(0) ? UTF8String.EMPTY
                        : vv.getString(0);
            }

            int groupCount = groupIds.size();
            if (result == null)
            {
                result = context.getVectorFactory()
                        .getMutableVector(ResolvedType.of(Type.String), groupCount);
            }

            List<UTF8String> strings = new ArrayList<>();
            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector selection = selections.getArray(i);

                UTF8String existing = result.size() > groupId ? (UTF8String) result.valueAsObject(i)
                        : null;
                ValueVector array = value.eval(input, selection, context);

                UTF8String groupValue = aggregate(strings, array, existing, delimiterString);
                if (groupValue == null)
                {
                    result.setNull(groupId);
                }
                else
                {
                    result.setString(groupId, groupValue);
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            return result;
        }
    }

    /** Aggregate provided vector */
    private static UTF8String aggregate(List<UTF8String> strings, ValueVector vector, UTF8String existing, UTF8String delimiter)
    {
        int size = vector.size();
        strings.clear();
        if (existing != null)
        {
            strings.add(existing);
        }
        for (int i = 0; i < size; i++)
        {
            if (vector.isNull(i))
            {
                continue;
            }
            strings.add(vector.getString(i));
        }
        if (strings.isEmpty())
        {
            return null;
        }
        return UTF8String.concat(delimiter, strings);
    }
}
