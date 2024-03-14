package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression.Type;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Sum aggregate. Sum all non null inputs */
class AggregateSumFunction extends ANumericAggregateFunction
{
    AggregateSumFunction()
    {
        super("sum");
    }

    @Override
    protected BaseAggregator createAggregator(IExpression expression)
    {
        return new SumAggregator(expression, getName());
    }

    private static class SumAggregator extends BaseAggregator
    {
        SumAggregator(IExpression expression, String name)
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
            int count = groupResult.size();
            for (int i = 0; i < count; i++)
            {
                if (groupResult.isNull(i))
                {
                    continue;
                }
                allNull = false;

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
    }

    @Override
    public String toString()
    {
        return "SUM";
    }
}
