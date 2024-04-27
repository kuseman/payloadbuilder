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
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/** IS (NOT?) NULL expression */
public class NullPredicateExpression implements INullPredicateExpression, Invertable
{
    private final IExpression expression;
    private final boolean not;

    public NullPredicateExpression(IExpression expression, boolean not)
    {
        this.expression = requireNonNull(expression, "expression");
        this.not = not;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isNot()
    {
        return not;
    }

    @Override
    public IExpression getInvertedExpression()
    {
        return new NullPredicateExpression(expression, !not);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);

        int rowCount = input.getRowCount();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.Boolean), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            boolean isNull = value.isNull(i);

            // Special case for Any types, verify the Any value's actual nullness
            if (!isNull
                    && value.type()
                            .getType() == Type.Any)
            {
                isNull = value.getAny(i) == null;
            }

            resultVector.setBoolean(i, not ? !isNull
                    : isNull);
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
        else if (obj instanceof NullPredicateExpression)
        {
            NullPredicateExpression that = (NullPredicateExpression) obj;
            return expression.equals(that.expression)
                    && not == that.not;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString() + " IS"
               + (not ? " NOT"
                       : "")
               + " NULL";
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString() + " IS"
               + (not ? " NOT"
                       : "")
               + " NULL";
    }
}
