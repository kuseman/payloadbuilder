package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralDateTimeExpression;

/** Date time expression */
public class LiteralDateTimeExpression extends LiteralExpression implements ILiteralDateTimeExpression
{
    private final EpochDateTime value;

    public LiteralDateTimeExpression(EpochDateTime value)
    {
        super(ResolvedType.of(Type.DateTime));
        this.value = requireNonNull(value, "value");
    }

    @Override
    public EpochDateTime getValue()
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
        return ValueVector.literalDateTime(value, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalDateTime(value, selection.size());
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
        else if (obj instanceof LiteralDateTimeExpression)
        {
            return value.equals(((LiteralDateTimeExpression) obj).value);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "'" + value + "'";
    }
}
