package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeOffsetExpression;

/** Date time offset expression */
public class LiteralDateTimeOffsetExpression extends LiteralExpression implements ILiteralDateTimeOffsetExpression
{
    private final EpochDateTimeOffset value;

    public LiteralDateTimeOffsetExpression(EpochDateTimeOffset value)
    {
        super(ResolvedType.of(Type.DateTimeOffset));
        this.value = requireNonNull(value, "value");
    }

    @Override
    public EpochDateTimeOffset getValue()
    {
        return value;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return ValueVector.literalDateTimeOffset(value, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalDateTimeOffset(value, selection.size());
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
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
        else if (obj instanceof LiteralDateTimeOffsetExpression)
        {
            return value.equals(((LiteralDateTimeOffsetExpression) obj).value);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "'" + value + "'";
    }
}
