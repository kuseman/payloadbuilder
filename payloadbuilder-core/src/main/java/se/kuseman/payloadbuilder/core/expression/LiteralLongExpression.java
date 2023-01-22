package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralLongExpression;

/** Long expression */
public class LiteralLongExpression extends LiteralExpression implements ILiteralLongExpression
{
    private final long value;

    LiteralLongExpression(String value)
    {
        this(Long.parseLong(value));
    }

    LiteralLongExpression(long value)
    {
        super(Type.Long);
        this.value = value;
    }

    @Override
    public long getValue()
    {
        return value;
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return ValueVector.literalLong(value, input.getRowCount());
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(value);
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
        else if (obj instanceof LiteralLongExpression)
        {
            return value == ((LiteralLongExpression) obj).value;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
