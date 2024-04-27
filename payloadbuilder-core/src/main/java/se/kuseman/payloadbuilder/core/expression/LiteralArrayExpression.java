package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralArrayExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Array expression */
public class LiteralArrayExpression extends LiteralExpression implements ILiteralArrayExpression
{
    private final ValueVector value;

    public LiteralArrayExpression(ValueVector value)
    {
        super(ResolvedType.array(requireNonNull(value, "value").type()));
        this.value = value;
    }

    @Override
    public ValueVector getValue()
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
        return ValueVector.literalArray(value, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalArray(value, selection.size());
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
        else if (obj instanceof LiteralArrayExpression)
        {
            LiteralArrayExpression that = (LiteralArrayExpression) obj;

            int size = value.size();
            if (size != that.value.size())
            {
                return false;
            }

            ValueVector[] vectors = new ValueVector[] { value, that.value };
            for (int i = 0; i < size; i++)
            {
                if (!VectorUtils.equals(vectors, i, i))
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
        return "array(" + IntStream.range(0, value.size())
                .mapToObj(i -> value.valueAsString(i))
                .collect(joining(", "))
               + ")";
    }
}
