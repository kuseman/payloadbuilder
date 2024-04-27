package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ISubscriptExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Subscript. index acces etc. ie "array[0]" */
public class SubscriptExpression implements ISubscriptExpression, HasAlias, HasColumnReference
{
    private final IExpression value;
    private final IExpression subscript;

    public SubscriptExpression(IExpression value, IExpression subscript)
    {
        this.value = requireNonNull(value, "value");
        this.subscript = requireNonNull(subscript, "subscript");
    }

    @Override
    public IExpression getValue()
    {
        return value;
    }

    @Override
    public IExpression getSubscript()
    {
        return subscript;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(value, subscript);
    }

    @Override
    public ColumnReference getColumnReference()
    {
        if (value instanceof HasColumnReference htsr)
        {
            return htsr.getColumnReference();
        }
        return null;
    }

    @Override
    public Alias getAlias()
    {
        if (value instanceof HasAlias)
        {
            return ((HasAlias) value).getAlias();
        }
        return HasAlias.Alias.EMPTY;
    }

    @Override
    public ResolvedType getType()
    {
        ResolvedType type = value.getType();
        ResolvedType subscriptType = subscript.getType();

        if (type.getType() == Type.Table)
        {
            // Subscript must be either integer/string/any
            if (!(subscriptType.getType() == Type.Int
                    || subscriptType.getType() == Type.String
                    || subscriptType.getType() == Type.Any))
            {
                failTable(subscriptType.getType());
            }

            // Filtered row in tuple vector
            if (subscriptType.getType() == Type.Int)
            {
                return ResolvedType.object(type.getSchema());
            }
            // Column
            else if (subscriptType.getType() == Type.String)
            {
                // TODO: Can we detect type, how to find column ?
                return ResolvedType.array(ResolvedType.of(Type.Any));
            }

            // Not known until runtime
            return ResolvedType.of(Type.Any);
        }
        else if (type.getType() == Type.Array)
        {
            // Subscript must be either integer/any
            if (!(subscriptType.getType() == Type.Int
                    || subscriptType.getType() == Type.Any))
            {
                failArray(subscriptType.getType());
            }
            return type.getSubType();
        }
        else if (type.getType() == Type.String)
        {
            // Subchar
            if (!(subscriptType.getType() == Type.Int
                    || subscriptType.getType() == Type.Any))
            {
                failString(subscriptType.getType());
            }

            return type;
        }
        else if (type.getType() == Type.Any)
        {
            if (!(subscriptType.getType() == Type.Int
                    || subscriptType.getType() == Type.Any))
            {
                failAny(subscriptType.getType());
            }

            return ResolvedType.of(Type.Any);
        }

        throw new IllegalArgumentException("Cannot subscript " + type.getType() + " with " + subscriptType);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector valueResult = value.eval(input, context);
        final ValueVector subscriptResult = subscript.eval(input, context);

        ResolvedType resultType = valueResult.type();

        if (resultType.getType() == Type.Table)
        {
            return subscriptTable((ExecutionContext) context, input.getRowCount(), valueResult, subscriptResult);
        }
        else if (resultType.getType() == Type.Array)
        {
            return subscriptArray((ExecutionContext) context, input.getRowCount(), valueResult, subscriptResult);
        }
        else if (resultType.getType() == Type.String)
        {
            return subscriptString((ExecutionContext) context, input.getRowCount(), valueResult, subscriptResult);
        }
        else if (resultType.getType() == Type.Any)
        {
            return subscriptAny((ExecutionContext) context, input.getRowCount(), valueResult, subscriptResult);
        }

        throw new IllegalArgumentException("Cannot subscript " + resultType + " with " + subscriptResult.type());
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    private ValueVector subscriptArray(ExecutionContext context, int rowCount, ValueVector value, ValueVector subscript)
    {
        // Subscript
        // int -> filtered row, type: value type
        // any -> runtime check for int

        Type subscriptType = subscript.type()
                .getType();

        if (!(subscriptType == Type.Int
                || subscriptType == Type.Any))
        {
            failArray(subscriptType);
        }

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(value.type()
                        .getSubType(), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            // Subscript or value is null => null
            if (value.isNull(i)
                    || subscript.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            ValueVector array = value.getArray(i);
            processArrayRow(array, subscriptType, subscript, i, resultVector);
        }

        return resultVector;
    }

    private void processArrayRow(ValueVector array, Type subscriptType, ValueVector subscript, int subscriptRow, MutableValueVector resultVector)
    {
        int index = -1;
        if (subscriptType == Type.Int)
        {
            index = subscript.getInt(subscriptRow);
        }
        else if (subscriptType == Type.Any)
        {
            Object val = subscript.getAny(subscriptRow);
            if (val instanceof Integer)
            {
                index = ((Integer) val).intValue();
            }
            else
            {
                failArray(val);
            }
        }
        else
        {
            failArray(subscriptType);
        }

        int arraySize = array.size();
        // Subscript from end
        // -1 is the last record
        if (index < 0)
        {
            index = arraySize - (-index);
        }

        // Out of bounds
        if (index >= arraySize
                || index < 0)
        {
            resultVector.setNull(subscriptRow);
        }
        else
        {
            resultVector.copy(subscriptRow, array, index);
        }
    }

    private ValueVector subscriptTable(ExecutionContext context, int rowCount, ValueVector value, ValueVector subscript)
    {
        // Subscript
        // int -> filtered row, type: value type
        // string -> column, type: column type
        // any -> runtime check or either int or string

        Type subscriptType = subscript.type()
                .getType();

        MutableValueVector resultVector = null;
        if (subscriptType == Type.Int)
        {
            resultVector = context.getVectorFactory()
                    .getMutableVector(ResolvedType.object(value.type()
                            .getSchema()), rowCount);
        }
        else if (subscriptType == Type.String)
        {
            // TODO: check actual runtime column type if subscript is constant
            resultVector = context.getVectorFactory()
                    .getMutableVector(ResolvedType.array(Type.Any), rowCount);
        }
        // Any
        else if (subscriptType == Type.Any)
        {
            resultVector = context.getVectorFactory()
                    .getMutableVector(ResolvedType.of(Type.Any), rowCount);
        }
        else
        {
            failTable(subscriptType);
        }

        for (int i = 0; i < rowCount; i++)
        {
            // Subscript or value is null => null
            if (value.isNull(i)
                    || subscript.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            TupleVector table = value.getTable(i);
            processTableRow(context, table, subscriptType, subscript, i, resultVector);
        }

        return resultVector;
    }

    private void processTableRow(ExecutionContext context, TupleVector table, Type subscriptType, ValueVector subscript, int subscriptRow, MutableValueVector resultVector)
    {
        Type currentSubscriptType = subscriptType;
        int row = -1;
        String column = null;
        if (subscriptType == Type.Any)
        {
            Object val = subscript.getAny(subscriptRow);
            if (val instanceof Integer)
            {
                currentSubscriptType = Type.Int;
                row = ((Integer) val).intValue();
            }
            else if (val instanceof String
                    || val instanceof UTF8String)
            {
                currentSubscriptType = Type.String;
                column = String.valueOf(val);
            }
            else
            {
                failTable(val);
            }
        }
        else if (subscriptType == Type.Int)
        {
            row = subscript.getInt(subscriptRow);
        }
        // String
        else if (subscriptType == Type.String)
        {
            column = String.valueOf(subscript.getString(subscriptRow));
        }
        else
        {
            failTable(subscriptType);
        }

        if (currentSubscriptType == Type.Int)
        {
            int currentRowCount = table.getRowCount();
            // Subscript from end
            // -1 is the last record
            if (row < 0)
            {
                row = currentRowCount - (-row);
            }

            // Out of bounds
            if (row >= currentRowCount
                    || row < 0)
            {
                resultVector.setNull(subscriptRow);
                return;
            }

            resultVector.setObject(subscriptRow, ObjectVector.wrap(table, row));
            return;
        }

        // String subscript
        int schemaSize = table.getSchema()
                .getSize();
        int ordinal = -1;
        for (int c = 0; c < schemaSize; c++)
        {
            Column col = table.getSchema()
                    .getColumns()
                    .get(c);
            if (col.getName()
                    .equalsIgnoreCase(column))
            {
                ordinal = c;
                break;
            }
        }
        if (ordinal < 0)
        {
            resultVector.setNull(subscriptRow);
            return;
        }
        resultVector.setArray(subscriptRow, table.getColumn(ordinal));
    }

    private ValueVector subscriptString(ExecutionContext context, int rowCount, ValueVector value, ValueVector subscript)
    {
        // Subscript
        // int -> filtered char, type: String
        // any -> runtime check for int

        Type subscriptType = subscript.type()
                .getType();

        if (!(subscriptType == Type.Int
                || subscriptType == Type.Any))
        {
            failString(subscriptType);
        }

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.String), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            // Subscript or value is null => null
            if (value.isNull(i)
                    || subscript.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            UTF8String ut8String = value.getString(i);
            processStringRow(String.valueOf(ut8String), subscriptType, subscript, i, resultVector);
        }

        return resultVector;
    }

    private void processStringRow(String string, Type subscriptType, ValueVector subscript, int subscriptRow, MutableValueVector resultVector)
    {
        int index = -1;
        if (subscriptType == Type.Int)
        {
            index = subscript.getInt(subscriptRow);
        }
        else if (subscriptType == Type.Any)
        {
            Object val = subscript.getAny(subscriptRow);
            if (val instanceof Integer)
            {
                index = ((Integer) val).intValue();
            }
            else
            {
                failString(val);
            }
        }
        else
        {
            failString(subscriptType);
        }

        int length = string.length();
        // Subscript from end
        // -1 is the last record
        if (index < 0)
        {
            index = length - (-index);
        }

        // Out of bounds
        if (index >= length
                || index < 0)
        {
            resultVector.setNull(subscriptRow);
        }
        else
        {
            resultVector.setString(subscriptRow, UTF8String.from(string.charAt(index)));
        }
    }

    private ValueVector subscriptAny(ExecutionContext context, int rowCount, ValueVector value, ValueVector subscript)
    {
        // Check each row for
        // - Table
        // - Array
        // - String

        Type subscriptType = subscript.type()
                .getType();

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.Any), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            // Subscript or value is null => null
            if (value.isNull(i)
                    || subscript.isNull(i))
            {
                resultVector.setNull(i);
                continue;
            }

            Object val = VectorUtils.convert(value.getAny(i));

            if (val instanceof TupleVector)
            {
                processTableRow(context, (TupleVector) val, subscriptType, subscript, i, resultVector);
            }
            else if (val instanceof ValueVector)
            {
                processArrayRow((ValueVector) val, subscriptType, subscript, i, resultVector);
            }
            else if (val instanceof String
                    || val instanceof UTF8String)
            {
                processStringRow(String.valueOf(val), subscriptType, subscript, i, resultVector);
            }
            else
            {
                failAny(val);
            }
        }

        return resultVector;
    }

    private void failTable(Type type)
    {
        throw new IllegalArgumentException("Cannot subscript a Table with " + type);
    }

    private void failTable(Object value)
    {
        throw new IllegalArgumentException("Cannot subscript a Table with value: " + value);
    }

    private void failArray(Type type)
    {
        throw new IllegalArgumentException("Cannot subscript an Array with " + type);
    }

    private void failArray(Object value)
    {
        throw new IllegalArgumentException("Cannot subscript an Array with value: " + value);
    }

    private void failString(Type type)
    {
        throw new IllegalArgumentException("Cannot subscript a String with " + type);
    }

    private void failString(Object value)
    {
        throw new IllegalArgumentException("Cannot subscript a String with value: " + value);
    }

    private void failAny(Type type)
    {
        throw new IllegalArgumentException("Cannot subscript Any with " + type);
    }

    private void failAny(Object value)
    {
        throw new IllegalArgumentException("Cannot subscript value: " + value);
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + value.hashCode();
        hashCode = hashCode * 37 + subscript.hashCode();
        return hashCode;
        // CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof SubscriptExpression)
        {
            SubscriptExpression that = (SubscriptExpression) obj;
            return value.equals(that.value)
                    && subscript.equals(that.subscript);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return value + "[" + subscript + "]";
    }

    @Override
    public String toVerboseString()
    {
        return value.toVerboseString() + "[" + subscript.toVerboseString() + "]";
    }
}
