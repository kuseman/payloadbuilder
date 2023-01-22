package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralFloatExpression;

/** Float expression */
public class LiteralFloatExpression extends LiteralExpression implements ILiteralFloatExpression
{
    private final float value;

    LiteralFloatExpression(String value)
    {
        this(Float.parseFloat(value));
    }

    public LiteralFloatExpression(float value)
    {
        super(Type.Float);
        this.value = value;
    }

    @Override
    public float getValue()
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
        return ValueVector.literalFloat(value, input.getRowCount());
    }

    @Override
    public int hashCode()
    {
        return Float.hashCode(value);
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
        else if (obj instanceof LiteralFloatExpression)
        {
            return value == ((LiteralFloatExpression) obj).value;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
