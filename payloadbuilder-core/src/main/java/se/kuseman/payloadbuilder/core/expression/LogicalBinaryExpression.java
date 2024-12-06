package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.FALSE;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.TRUE;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** AND/OR expression */
public class LogicalBinaryExpression extends ABinaryExpression implements ILogicalBinaryExpression
{
    private final Type type;

    public LogicalBinaryExpression(Type type, IExpression left, IExpression right)
    {
        super(left, right);
        this.type = requireNonNull(type, "type");
    }

    @Override
    public Type getLogicalType()
    {
        return type;
    }

    @Override
    public IExpression fold()
    {
        // https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
        // AND - false if either side is false or null
        if (type == Type.AND)
        {
            if (isFalse(left)
                    || isFalse(right))
            {
                return FALSE;
            }
            else if (isTrue(left))
            {
                return right;
            }
            else if (isTrue(right))
            {
                return left;
            }
        }
        // OR 3vl True if either side is true or null */
        else if (isFalse(left))
        {
            return right;
        }
        else if (isFalse(right))
        {
            return left;
        }
        else if (isTrue(left)
                || isTrue(right))
        {
            return TRUE;
        }

        return this;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        int size = selection.size();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Column.Type.Boolean), size);

        boolean isAnd = type == Type.AND;

        // First evaluate the left side and set all null/true/false that don't need the right side
        ValueVector lvv = left.eval(input, selection, context);
        int[] selectionRows = null;
        int[] inputRows = null;

        int index = 0;
        for (int i = 0; i < size; i++)
        {
            // Optimize for lazy eval, which means we should not touch right side
            // either for isNull or getBoolean if we don't need
            boolean leftNull = lvv.isNull(i);
            if (!leftNull)
            {
                boolean lv = lvv.getBoolean(i);

                // true AND ? => Continue
                // false AND ? => false
                // true OR ? => true
                // false OR ? => Continue

                // false AND ? => false
                if (!lv
                        && isAnd)
                {
                    resultVector.setBoolean(i, false);
                    continue;
                }
                // true OR ? => true
                else if (lv
                        && !isAnd)
                {
                    resultVector.setBoolean(i, true);
                    continue;
                }
            }
            if (selectionRows == null)
            {
                selectionRows = new int[size - i];
                inputRows = new int[size - i];
            }
            selectionRows[index] = selection.getInt(i);
            inputRows[index] = i;
            index++;
        }

        // All results could be resolved with only left side, return
        if (selectionRows == null)
        {
            return resultVector;
        }

        // Now eval the right side with the needed rows
        ValueVector rightSelection = VectorUtils.convertToSelectionVector(selectionRows, index);
        ValueVector rvv = right.eval(input, rightSelection, context);

        for (int i = 0; i < index; i++)
        {
            int inputRow = inputRows[i];
            boolean leftNull = lvv.isNull(inputRow);
            boolean rightNull = rvv.isNull(i);
            boolean rv = !rightNull ? rvv.getBoolean(i)
                    : false;

            if (leftNull
                    && rightNull)
            {
                // null AND/OR null => null
                resultVector.setNull(inputRow);
                continue;
            }
            else if (rightNull)
            {
                // true AND null
                // false OR null
                resultVector.setNull(inputRow);
                continue;
            }
            else if (leftNull)
            {
                // null AND false
                if (!rv
                        && isAnd)
                {
                    resultVector.setBoolean(inputRow, false);
                    continue;
                }
                // null OR true
                else if (rv
                        && !isAnd)
                {
                    resultVector.setBoolean(inputRow, true);
                    continue;
                }

                // null AND true
                // null OR false
                resultVector.setNull(inputRow);
                continue;
            }

            boolean lv = lvv.getBoolean(inputRow);
            boolean result = isAnd ? lv
                    && rv
                    : lv
                            || rv;

            resultVector.setBoolean(inputRow, result);
        }

        return resultVector;
    }

    private boolean isTrue(IExpression expression)
    {
        return TRUE.equals(expression);
    }

    private boolean isFalse(IExpression expression)
    {
        return FALSE.equals(expression);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        if (obj instanceof LogicalBinaryExpression)
        {
            LogicalBinaryExpression that = (LogicalBinaryExpression) obj;
            return super.equals(obj)
                    && type == that.type;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", left, type, right);
    }

    @Override
    public String toVerboseString()
    {
        return String.format("%s %s %s", left.toVerboseString(), type, right.toVerboseString());
    }

}
