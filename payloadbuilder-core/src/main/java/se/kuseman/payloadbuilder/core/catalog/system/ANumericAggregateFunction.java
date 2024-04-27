package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.SelectedValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/** Base class for numeric aggregate functions. SUM/AVG */
abstract class ANumericAggregateFunction extends ScalarFunctionInfo
{
    ANumericAggregateFunction(String name)
    {
        super(name, FunctionType.SCALAR_AGGREGATE);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
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
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        // int: 10,20,30,40
        boolean isArray = false;
        ValueVector vv = arguments.get(0)
                .eval(input, context);
        Type resultType = vv.type()
                .getType();
        if (resultType == Column.Type.Array)
        {
            isArray = true;
            resultType = vv.type()
                    .getSubType()
                    .getType();
        }

        int groupCount = input.getRowCount();
        BaseAggregator aggregator = createAggregator(arguments.get(0));
        aggregator.resultType = resultType;
        aggregator.size = groupCount;

        // Append all rows as single groups
        for (int i = 0; i < groupCount; i++)
        {
            // Arrays then those should be aggregated
            if (isArray)
            {
                aggregator.append(vv.getArray(i), i, context);
                continue;
            }

            aggregator.append(SelectedValueVector.select(vv, ValueVector.literalInt(i, 1)), i, context);
        }

        return aggregator.combine(context);
    }

    @Override
    public IAggregator createAggregator(AggregateMode mode, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }
        return createAggregator(arguments.get(0));
    }

    protected BaseAggregator createAggregator(IExpression expression)
    {
        return null;
    }

    /** Base aggregator for numeric aggregate functions that takes away much of the boiler plate */
    protected abstract static class BaseAggregator implements IAggregator
    {
        private final IExpression expression;
        private final String name;
        protected Type resultType = null;
        protected int size = 0;
        protected BitSet valueSetBitSet;
        protected IntList intResult;
        protected LongList longResult;
        protected FloatList floatResult;
        protected DoubleList doubleResult;
        protected List<Object> objectResult;

        BaseAggregator(IExpression expression, String name)
        {
            this.expression = expression;
            this.name = name;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            int groupCount = groupIds.size();

            boolean typeIsSet = resultType != null;
            ValueVector[] result = new ValueVector[groupCount];
            // Evaluate all groups and determine result type if needed, this is only performed on first vector
            // all other vectors will be implicitly casted

            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector selection = selections.getArray(i);
                size = Math.max(size, groupId + 1);

                if (selection.size() == 0)
                {
                    continue;
                }

                ValueVector vv = expression.eval(input, selection, context);
                result[i] = vv;

                // First vector de
                if (!typeIsSet)
                {
                    if (resultType == null)
                    {
                        resultType = vv.type()
                                .getType();
                    }
                    else
                    {
                        resultType = getResultType(name, resultType, vv.type());
                    }
                }
            }

            // Append each evaluated group
            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector vv = result[i];
                if (vv != null)
                {
                    append(vv, groupId, context);
                }
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.of(resultType);
                }

                @Override
                public int size()
                {
                    return size;
                }

                @Override
                public boolean isNull(int row)
                {
                    if (objectResult != null)
                    {
                        return objectResult.get(row) == null;
                    }
                    return valueSetBitSet == null
                            || !valueSetBitSet.get(row);
                }

                @Override
                public int getInt(int row)
                {
                    return intResult.getInt(row);
                }

                @Override
                public long getLong(int row)
                {
                    return longResult.getLong(row);
                }

                @Override
                public float getFloat(int row)
                {
                    return floatResult.getFloat(row);
                }

                @Override
                public double getDouble(int row)
                {
                    return doubleResult.getDouble(row);
                }

                @Override
                public Decimal getDecimal(int row)
                {
                    return (Decimal) objectResult.get(row);
                }

                @Override
                public Object getAny(int row)
                {
                    return objectResult.get(row);
                }
            };
        }

        private void setValueBit(int groupId)
        {
            if (valueSetBitSet == null)
            {
                valueSetBitSet = new BitSet();
            }
            valueSetBitSet.set(groupId);
        }

        protected int getInt(int groupId)
        {
            if (intResult == null)
            {
                intResult = new IntArrayList(size);
            }
            intResult.size(size);
            return intResult.getInt(groupId);
        }

        protected void setInt(int groupId, int value)
        {
            setValueBit(groupId);
            intResult.set(groupId, value);
        }

        protected long getLong(int groupId)
        {
            if (longResult == null)
            {
                longResult = new LongArrayList(size);
            }
            longResult.size(size);
            return longResult.getLong(groupId);
        }

        protected void setLong(int groupId, long value)
        {
            setValueBit(groupId);
            longResult.set(groupId, value);
        }

        protected float getFloat(int groupId)
        {
            if (floatResult == null)
            {
                floatResult = new FloatArrayList(size);
            }
            floatResult.size(size);
            return floatResult.getFloat(groupId);
        }

        protected void setFloat(int groupId, float value)
        {
            setValueBit(groupId);
            floatResult.set(groupId, value);
        }

        protected double getDouble(int groupId)
        {
            if (doubleResult == null)
            {
                doubleResult = new DoubleArrayList(size);
            }
            doubleResult.size(size);
            return doubleResult.getDouble(groupId);
        }

        protected void setDouble(int groupId, double value)
        {
            setValueBit(groupId);
            doubleResult.set(groupId, value);
        }

        @SuppressWarnings("unchecked")
        protected <T> T getObject(int groupId)
        {
            if (objectResult == null)
            {
                objectResult = new ArrayList<>(size);
            }

            if (objectResult.size() < size)
            {
                ((ArrayList<?>) objectResult).ensureCapacity(size);
                objectResult.addAll(Collections.nCopies(size - objectResult.size(), null));
            }

            return (T) objectResult.get(groupId);
        }

        protected void setObject(int groupId, Object value)
        {
            objectResult.set(groupId, value);
        }

        /** Do the actual aggregation of this function for provided group */
        protected abstract void append(ValueVector groupResult, int group, IExecutionContext context);
    }

    private static Type getResultType(String name, Type current, ResolvedType next)
    {
        Type type = next.getType();

        if (next.getType() == Column.Type.Array)
        {
            type = next.getSubType()
                    .getType();
        }
        // Type object then we need to use reflective SUM. Ie BigDecimal etc.
        if (!(type.isNumber()
                || type == Column.Type.Any))
        {
            throw new IllegalArgumentException(name + " doesn't support type " + type);
        }

        if (type.getPrecedence() > current.getPrecedence())
        {
            return type;
        }
        return current;
    }
}
