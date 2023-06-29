package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralIntegerExpression;

/** Integer expression */
public class LiteralIntegerExpression extends LiteralExpression implements ILiteralIntegerExpression
{
    private final int value;

    LiteralIntegerExpression(String value)
    {
        this(Integer.parseInt(value));
    }

    public LiteralIntegerExpression(int value)
    {
        super(ResolvedType.of(Type.Int));
        this.value = value;
    }

    @Override
    public int getValue()
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
        return ValueVector.literalInt(value, input.getRowCount());
    }

    @Override
    public int hashCode()
    {
        return value;
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
        else if (obj instanceof LiteralIntegerExpression)
        {
            return value == ((LiteralIntegerExpression) obj).value;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }
}
