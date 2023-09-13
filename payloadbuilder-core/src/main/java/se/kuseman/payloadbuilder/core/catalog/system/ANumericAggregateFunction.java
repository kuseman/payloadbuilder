package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

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
        ValueVector vv = arguments.get(0)
                .eval(input, context);
        Type resultType = vv.type()
                .getType();
        if (resultType == Column.Type.Array)
        {
            resultType = vv.type()
                    .getSubType()
                    .getType();
        }

        if (resultType == Column.Type.Int
                || resultType == Column.Type.Long)
        {
            return aggregateInteger(resultType, vv);
        }
        else if (resultType == Column.Type.Float
                || resultType == Column.Type.Double)
        {
            return aggregateDecimalPrimitive(resultType, vv);
        }

        return aggregateObjects(vv);
    }

    @Override
    public ValueVector evalAggregate(IExecutionContext context, AggregateMode mode, ValueVector groups, String catalogAlias, List<IExpression> arguments)
    {
        if (mode == AggregateMode.DISTINCT)
        {
            throw new UnsupportedOperationException(getName() + " DISTINCT is unsupported");
        }

        if (groups.type()
                .getType() != Column.Type.Table)
        {
            throw new IllegalArgumentException("Wrong type of input vector, expected tuple vector but got: " + groups.type());
        }

        IExpression expression = arguments.get(0);

        int size = groups.size();
        // Start out with int as result
        Type resultType = null;
        List<ValueVector> vectors = new ArrayList<>(groups.size());
        for (int i = 0; i < size; i++)
        {
            ValueVector vv = expression.eval(groups.getTable(i), context);
            if (resultType == null)
            {
                resultType = vv.type()
                        .getType();
            }
            else
            {
                resultType = getResultType(resultType, vv.type());
            }
            vectors.add(vv);
        }
        if (resultType == Column.Type.Int
                || resultType == Column.Type.Long)
        {
            return aggregateInteger(resultType, of(vectors, resultType));
        }
        else if (resultType == Column.Type.Float
                || resultType == Column.Type.Double)
        {
            return aggregateDecimalPrimitive(resultType, of(vectors, resultType));
        }
        else if (resultType == Column.Type.Decimal)
        {
            return aggregateDecimal(of(vectors, resultType));
        }

        return aggregateObjects(of(vectors, resultType));
    }

    private Type getResultType(Type current, ResolvedType next)
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
            throw new IllegalArgumentException(getName() + " doesn't support type " + type);
        }

        if (type.getPrecedence() > current.getPrecedence())
        {
            return type;
        }
        return current;
    }

    private ValueVector of(List<ValueVector> vectors, Type resultType)
    {
        final int size = vectors.size();
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.array(ResolvedType.of(resultType));
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public ValueVector getArray(int row)
            {
                return vectors.get(row);
            }
        };
    }

    /** Creates a {@link IntAggregator} */
    protected abstract IntAggregator createIntAggregator();

    /** Creates a {@link LongAggregator} */
    protected abstract LongAggregator createLongAggregator();

    /** Creates a {@link FloatAggregator} */
    protected abstract FloatAggregator createFloatAggregator();

    /** Creates a {@link DoubleAggregator} */
    protected abstract DoubleAggregator createDoubleAggregator();

    /** Creates a {@link DecimalAggregator} */
    protected abstract DecimalAggregator createDecimalAggregator();

    /** Creates a {@link ObjectAggregator} */
    protected abstract ObjectAggregator createObjectAggregator();

    /** Aggregates integers */
    protected interface IntAggregator
    {
        /** Return the identity */
        default int getIdentity()
        {
            return 0;
        }

        /** Aggregate current and next value */
        int aggregate(int current, int next);

        /** Combine the result */
        default int combine(int result)
        {
            return result;
        }
    }

    /** Aggregates longs */
    protected interface LongAggregator
    {
        /** Return the identity */
        default long getIdentity()
        {
            return 0;
        }

        /** Aggregate current and next value */
        long aggregate(long current, long next);

        /** Combine the result */
        default long combine(long result)
        {
            return result;
        }
    }

    /** Aggregates floats */
    protected interface FloatAggregator
    {
        /** Return the identity */
        default float getIdentity()
        {
            return 0;
        }

        /** Aggregate current and next value */
        float aggregate(float current, float next);

        /** Combine the result */
        default float combine(float result)
        {
            return result;
        }
    }

    /** Aggregates doubles */
    protected interface DoubleAggregator
    {
        /** Return the identity */
        default double getIdentity()
        {
            return 0;
        }

        /** Aggregate current and next value */
        double aggregate(double current, double next);

        /** Combine the result */
        default double combine(double result)
        {
            return result;
        }
    }

    /** Aggregates doubles */
    protected interface DecimalAggregator
    {
        /** Return the identity */
        default Decimal getIdentity()
        {
            return null;
        }

        /** Aggregate current and next value */
        Decimal aggregate(Decimal current, Decimal next);

        /** Combine the result */
        default Decimal combine(Decimal result)
        {
            return result;
        }
    }

    /** Aggregates objects */
    protected interface ObjectAggregator
    {
        /** Return the identity */
        default Object getIdentity()
        {
            return null;
        }

        /** Aggregate current and next value */
        Object aggregate(Object current, Object next);

        /** Combine the result */
        default Object combine(Object result)
        {
            return result;
        }
    }

    private ValueVector aggregateObjects(ValueVector values)
    {
        Type type = values.type()
                .getType();
        final int size = values.size();
        final Object[] results = new Object[size];
        for (int i = 0; i < size; i++)
        {
            ObjectAggregator aggregator = createObjectAggregator();

            ValueVector v;
            int start = 0;
            int end;

            // The value should be a ValueVector that should be aggregated
            if (type == Column.Type.Array)
            {
                v = values.getArray(i);
                end = v.size();
            }
            // Scalar mode then we aggregate the whole input vector
            else
            {
                // Only one value should be aggregated
                v = values;
                start = i;
                end = i + 1;
            }

            Object result = aggregator.getIdentity();
            for (int r = start; r < end; r++)
            {
                if (v.isNull(r))
                {
                    continue;
                }
                result = aggregator.aggregate(result, v.valueAsObject(r));
            }
            if (result != null)
            {
                results[i] = aggregator.combine(result);
            }
        }

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                // TODO: could be converted to a primitive type by checking all values in array
                return ResolvedType.of(Column.Type.Any);
            }

            @Override
            public boolean isNull(int row)
            {
                return results[row] == null;
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public Object getAny(int row)
            {
                return results[row];
            }
        };
    }

    /**
     * Aggregates integer values
     */
    private ValueVector aggregateInteger(final Type resultType, ValueVector values)
    {
        Type type = values.type()
                .getType();
        final int size = values.size();
        boolean isLong = resultType == Column.Type.Long;
        int[] intArr = isLong ? null
                : new int[size];
        long[] longArr = isLong ? new long[size]
                : null;
        BitSet nulls = null;

        for (int i = 0; i < size; i++)
        {
            LongAggregator longAggregator = isLong ? createLongAggregator()
                    : null;
            IntAggregator intAggregator = isLong ? null
                    : createIntAggregator();

            ValueVector v;
            int start = 0;
            int end;

            // The value should be a ValueVector that should be aggregated
            if (type == Column.Type.Array)
            {
                v = values.getArray(i);
                end = v.size();
            }
            // Scalar mode then we aggregate the whole input vector
            else
            {
                // Only one value should be aggregated
                v = values;
                start = i;
                end = i + 1;
            }
            // CSOFF
            int iR = isLong ? 0
                    : intAggregator.getIdentity();
            long lR = isLong ? longAggregator.getIdentity()
                    : 0;
            // CSON
            boolean allNulls = true;
            for (int r = start; r < end; r++)
            {
                if (v.isNull(r))
                {
                    continue;
                }

                allNulls = false;
                if (isLong)
                {
                    lR = longAggregator.aggregate(lR, v.getLong(r));
                }
                else
                {
                    iR = intAggregator.aggregate(iR, v.getInt(r));
                }
            }

            if (allNulls)
            {
                if (nulls == null)
                {
                    nulls = new BitSet();
                }
                nulls.set(i);
            }
            else
            {
                if (isLong)
                {
                    longArr[i] = longAggregator.combine(lR);
                }
                else
                {
                    intArr[i] = intAggregator.combine(iR);
                }
            }
        }

        final int[] intResult = intArr;
        final long[] longResult = longArr;
        final BitSet nullBitSet = nulls;
        return new ValueVector()
        {
            @Override
            public boolean isNull(int row)
            {
                if (nullBitSet == null)
                {
                    return false;
                }
                return nullBitSet.get(row);
            }

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
            public int getInt(int row)
            {
                if (resultType == Column.Type.Long)
                {
                    return (int) longResult[row];
                }

                return intResult[row];
            }

            @Override
            public long getLong(int row)
            {
                if (resultType == Column.Type.Int)
                {
                    return intResult[row];
                }

                return longResult[row];
            }
        };
    }

    private ValueVector aggregateDecimal(ValueVector values)
    {
        Column.Type type = values.type()
                .getType();
        final int size = values.size();
        final Decimal[] result = new Decimal[size];
        BitSet nulls = null;

        for (int i = 0; i < size; i++)
        {
            DecimalAggregator aggregator = createDecimalAggregator();

            ValueVector v;
            int start = 0;
            int end;

            // The value should be a ValueVector that should be aggregated
            if (type == Column.Type.Array)
            {
                v = values.getArray(i);
                end = v.size();
            }
            // Scalar mode then we aggregate the whole input vector
            else
            {
                // Only one value should be aggregated
                v = values;
                start = i;
                end = i + 1;
            }

            Decimal d = aggregator.getIdentity();
            boolean allNulls = true;
            for (int r = start; r < end; r++)
            {
                if (v.isNull(r))
                {
                    continue;
                }

                allNulls = false;
                d = aggregator.aggregate(d, v.getDecimal(r));
            }
            if (allNulls)
            {
                if (nulls == null)
                {
                    nulls = new BitSet(size);
                }
                nulls.set(i);
            }
            else
            {
                result[i] = aggregator.combine(d);
            }
        }
        final BitSet nullBitSet = nulls;
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Column.Type.Decimal);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public boolean isNull(int row)
            {
                if (nullBitSet == null)
                {
                    return false;
                }
                return nullBitSet.get(row);
            }

            @Override
            public Decimal getDecimal(int row)
            {
                return result[row];
            }
        };
    }

    /**
     * Aggregate decimal primitive values
     */
    private ValueVector aggregateDecimalPrimitive(final Column.Type resultType, ValueVector values)
    {
        Column.Type type = values.type()
                .getType();
        final int size = values.size();
        boolean isDouble = resultType == Column.Type.Double;
        float[] floatArr = isDouble ? null
                : new float[size];
        double[] doubleArr = isDouble ? new double[size]
                : null;
        BitSet nulls = null;

        for (int i = 0; i < size; i++)
        {
            FloatAggregator floatAggregator = isDouble ? null
                    : createFloatAggregator();
            DoubleAggregator doubleAggregator = isDouble ? createDoubleAggregator()
                    : null;

            ValueVector v;
            int start = 0;
            int end;

            // The value should be a ValueVector that should be aggregated
            if (type == Column.Type.Array)
            {
                v = values.getArray(i);
                end = v.size();
            }
            // Scalar mode then we aggregate the whole input vector
            else
            {
                // Only one value should be aggregated
                v = values;
                start = i;
                end = i + 1;
            }

            // CSOFF
            float fR = isDouble ? 0
                    : floatAggregator.getIdentity();
            double dR = isDouble ? doubleAggregator.getIdentity()
                    : 0;
            // CSON
            boolean allNulls = true;
            for (int r = start; r < end; r++)
            {
                if (v.isNull(r))
                {
                    continue;
                }

                allNulls = false;
                if (isDouble)
                {
                    dR = doubleAggregator.aggregate(dR, (v.type()
                            .getType() == Column.Type.Float ? v.getFloat(r)
                                    : v.getDouble(r)));
                }
                else
                {
                    fR = floatAggregator.aggregate(fR, v.getFloat(r));
                }
            }

            if (allNulls)
            {
                if (nulls == null)
                {
                    nulls = new BitSet(size);
                }
                nulls.set(i);
            }
            else
            {
                if (isDouble)
                {
                    doubleArr[i] = doubleAggregator.combine(dR);
                }
                else
                {
                    floatArr[i] = floatAggregator.combine(fR);
                }
            }
        }

        final float[] floatResult = floatArr;
        final double[] doubleResult = doubleArr;
        final BitSet nullBitSet = nulls;
        return new ValueVector()
        {
            @Override
            public boolean isNull(int row)
            {
                if (nullBitSet == null)
                {
                    return false;
                }
                return nullBitSet.get(row);
            }

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
            public float getFloat(int row)
            {
                if (resultType == Column.Type.Double)
                {
                    return (float) doubleResult[row];
                }

                return floatResult[row];
            }

            @Override
            public double getDouble(int row)
            {
                if (resultType == Column.Type.Float)
                {
                    return floatResult[row];
                }

                return doubleResult[row];
            }
        };
    }
}
