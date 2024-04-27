package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILogicalNotExpression;

/** Logical NOT */
public class LogicalNotExpression implements ILogicalNotExpression, Invertable
{
    private final IExpression expression;

    public LogicalNotExpression(IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public IExpression getInvertedExpression()
    {
        // NOT NOT a => a
        return expression;
    }

    @Override
    public IExpression fold()
    {
        // Invert expressions
        if (expression.isConstant())
        {
            ValueVector vector = expression.eval(null);
            if (vector.isNull(0))
            {
                // NOT NULL => NULL
                return new LiteralNullExpression(ResolvedType.of(Type.Boolean));
            }

            return !vector.getBoolean(0) ? LiteralBooleanExpression.TRUE
                    : LiteralBooleanExpression.FALSE;
        }
        else if (expression instanceof Invertable i)
        {
            return i.getInvertedExpression();
        }
        return this;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ValueVector vector = expression.eval(input, context);

        int rowCount = input.getRowCount();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.Boolean), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            boolean isNull = vector.isNull(i);
            if (isNull)
            {
                resultVector.setNull(i);
            }
            else
            {
                resultVector.setBoolean(i, !vector.getBoolean(i));
            }
        }

        return resultVector;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Boolean);
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
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
        else if (obj instanceof LogicalNotExpression)
        {
            return expression.equals(((LogicalNotExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "NOT " + expression;
    }
}
