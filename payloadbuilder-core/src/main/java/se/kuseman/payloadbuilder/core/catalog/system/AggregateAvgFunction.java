package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression.Type;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/** Avg aggregate. Return the avg value among values */
class AggregateAvgFunction extends ANumericAggregateFunction
{
    AggregateAvgFunction()
    {
        super("avg");
    }

    @Override
    protected BaseAggregator createAggregator(IExpression expression)
    {
        return new AvgAggregator(expression, getName());
    }

    private static class AvgAggregator extends BaseAggregator
    {
        private IntList counts;

        AvgAggregator(IExpression expression, String name)
        {
            super(expression, name);
        }

        @Override
        protected void append(ValueVector groupResult, int group, IExecutionContext context)
        {
            int intSum = 0;
            long longSum = 0;
            float floatSum = 0;
            double doubleSum = 0;
            Decimal decimalSum = null;
            Object anySum = null;

            boolean allNull = true;
            int rowCount = 0;
            int count = groupResult.size();
            for (int i = 0; i < count; i++)
            {
                if (groupResult.isNull(i))
                {
                    continue;
                }
                allNull = false;
                rowCount++;
                switch (resultType)
                {
                    case Int:
                        intSum = Math.addExact(intSum, groupResult.getInt(i));
                        break;
                    case Long:
                        longSum = Math.addExact(longSum, groupResult.getLong(i));
                        break;
                    case Float:
                        floatSum += groupResult.getFloat(i);
                        break;
                    case Double:
                        doubleSum += groupResult.getDouble(i);
                        break;
                    case Decimal:
                        if (decimalSum == null)
                        {
                            decimalSum = groupResult.getDecimal(i);
                        }
                        else
                        {
                            decimalSum = decimalSum.processArithmetic(groupResult.getDecimal(i), Type.ADD);
                        }
                        break;
                    default:
                        if (anySum == null)
                        {
                            anySum = groupResult.getAny(i);
                        }
                        else
                        {
                            anySum = ExpressionMath.add(anySum, groupResult.getAny(i));
                        }
                        break;
                }
            }

            if (allNull)
            {
                return;
            }

            if (counts == null)
            {
                counts = new IntArrayList(size);
            }
            counts.size(size);
            counts.set(group, counts.getInt(group) + rowCount);

            switch (resultType)
            {
                case Int:
                    setInt(group, intSum + getInt(group));
                    break;
                case Long:
                    setLong(group, longSum + getLong(group));
                    break;
                case Float:
                    setFloat(group, floatSum + getFloat(group));
                    break;
                case Double:
                    setDouble(group, doubleSum + getDouble(group));
                    break;
                case Decimal:
                    Decimal currentDecimal = (Decimal) getObject(group);
                    if (currentDecimal == null)
                    {
                        setObject(group, decimalSum);
                    }
                    else
                    {
                        setObject(group, currentDecimal.processArithmetic(decimalSum, Type.ADD));
                    }
                    break;
                default:
                    Object currentObject = getObject(group);
                    if (currentObject == null)
                    {
                        setObject(group, anySum);
                    }
                    else
                    {
                        setObject(group, ExpressionMath.add(currentObject, anySum));
                    }
                    break;
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            // Calculate the averages before combining the result
            for (int group = 0; group < size; group++)
            {
                int count = counts.getInt(group);
                if (count == 0)
                {
                    continue;
                }
                switch (resultType)
                {
                    case Int:
                        setInt(group, getInt(group) / count);
                        break;
                    case Long:
                        setLong(group, getLong(group) / count);
                        break;
                    case Float:
                        setFloat(group, getFloat(group) / count);
                        break;
                    case Double:
                        setDouble(group, getDouble(group) / count);
                        break;
                    case Decimal:
                        Decimal currentDecimal = (Decimal) getObject(group);
                        setObject(group, currentDecimal.processArithmetic(Decimal.from(count), Type.DIVIDE));
                        break;
                    default:
                        Object currentObject = getObject(group);
                        setObject(group, ExpressionMath.divide(currentObject, count));
                        break;
                }
            }
            return super.combine(context);
        }
    }

    @Override
    public String toString()
    {
        return "AVG";
    }
}
