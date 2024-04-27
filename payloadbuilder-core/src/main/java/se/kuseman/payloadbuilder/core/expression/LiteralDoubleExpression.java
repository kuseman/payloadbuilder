package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralDoubleExpression;

/** Double expression */
public class LiteralDoubleExpression extends LiteralExpression implements ILiteralDoubleExpression
{
    private final double value;

    public LiteralDoubleExpression(String value)
    {
        this(Double.parseDouble(value));
    }

    public LiteralDoubleExpression(double value)
    {
        super(ResolvedType.of(Type.Double));
        this.value = value;
    }

    @Override
    public double getValue()
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
        return ValueVector.literalDouble(value, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalDouble(value, selection.size());
    }

    @Override
    public int hashCode()
    {
        return Double.hashCode(value);
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
        else if (obj instanceof LiteralDoubleExpression)
        {
            return value == ((LiteralDoubleExpression) obj).value;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
