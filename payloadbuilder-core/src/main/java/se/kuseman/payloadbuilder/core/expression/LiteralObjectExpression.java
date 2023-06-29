package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralObjectExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Object expression */
public class LiteralObjectExpression extends LiteralExpression implements ILiteralObjectExpression
{
    private final ObjectVector value;

    public LiteralObjectExpression(ObjectVector value)
    {
        super(ResolvedType.object(requireNonNull(value, "value").getSchema()));
        this.value = value;
    }

    @Override
    public ObjectVector getValue()
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
        return ValueVector.literalObject(value, input.getRowCount());
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
        else if (obj instanceof LiteralObjectExpression)
        {
            LiteralObjectExpression that = (LiteralObjectExpression) obj;

            int size = value.getSchema()
                    .getSize();
            if (size != that.value.getSchema()
                    .getSize())
            {
                return false;
            }

            for (int i = 0; i < size; i++)
            {
                ValueVector[] vectors = new ValueVector[] { value.getValue(i), that.value.getValue(i) };
                if (!VectorUtils.equals(vectors, value.getRow(), that.value.getRow()))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "" + value;
    }
}
