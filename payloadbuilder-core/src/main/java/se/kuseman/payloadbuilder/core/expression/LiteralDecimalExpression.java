package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.ILiteralDecimalExpression;

/** Decimal expression */
public class LiteralDecimalExpression extends LiteralExpression implements ILiteralDecimalExpression
{
    private final Decimal value;

    public LiteralDecimalExpression(Decimal value)
    {
        super(ResolvedType.of(Type.Decimal));
        this.value = requireNonNull(value, "value");
    }

    @Override
    public Decimal getValue()
    {
        return value;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return ValueVector.literalDecimal(value, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalDecimal(value, selection.size());
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
        else if (obj instanceof LiteralDecimalExpression)
        {
            return value.equals(((LiteralDecimalExpression) obj).value);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
