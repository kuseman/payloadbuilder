package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralBooleanExpression;

/** Boolean expression */
public class LiteralBooleanExpression extends LiteralExpression implements ILiteralBooleanExpression
{
    public static LiteralBooleanExpression TRUE = new LiteralBooleanExpression(true);
    public static LiteralBooleanExpression FALSE = new LiteralBooleanExpression(false);

    private final boolean value;

    LiteralBooleanExpression(boolean value)
    {
        super(ResolvedType.of(Type.Boolean));
        this.value = value;
    }

    @Override
    public boolean getValue()
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
        return ValueVector.literalBoolean(value, input.getRowCount());
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode(value);
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
        else if (obj instanceof LiteralBooleanExpression)
        {
            return value == ((LiteralBooleanExpression) obj).value;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
