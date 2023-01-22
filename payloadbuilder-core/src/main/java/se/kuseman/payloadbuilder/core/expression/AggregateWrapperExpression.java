package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** An aggregate expression that wrapps a ordinary expression and turns it into an aggregate result */
public class AggregateWrapperExpression implements IAggregateExpression, HasAlias
{
    private final IExpression expression;

    /**
     * Should we return a single value for each group? Ie. is this column one of the aggregate expressions.
     * 
     * <pre>
     *  select col1, count(*)   <---- col1 is singleValue since we have that one in the group clause
     *  from table
     *  group by col1
     *  
     *  Ie. If all referenced expressions down the tree references only the group by expressions we return a single value
     * </pre>
     */
    private final boolean singleValue;

    /** Internal expression used in plan operators only */
    private final boolean internal;

    public AggregateWrapperExpression(IExpression expression, boolean singleValue, boolean internal)
    {
        this.expression = requireNonNull(expression, "expression");
        this.singleValue = singleValue;
        this.internal = internal;
    }

    public boolean isSingleValue()
    {
        return singleValue;
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isInternal()
    {
        return internal;
    }

    @Override
    public ColumnReference getColumnReference()
    {
        return expression.getColumnReference();
    }

    @Override
    public Alias getAlias()
    {
        if (expression instanceof HasAlias)
        {
            return ((HasAlias) expression).getAlias();
        }
        return HasAlias.Alias.EMPTY;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public ResolvedType getType()
    {
        IExpression e = expression;
        if (e instanceof AliasExpression)
        {
            e = ((AliasExpression) expression).getExpression();
        }
        if (e instanceof IAggregateExpression)
        {
            return ((IAggregateExpression) e).getType();
        }

        // We return a value vector if it's not a single value
        return singleValue ? expression.getType()
                : ResolvedType.valueVector(expression.getType());
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        if (visitor instanceof ICoreExpressionVisitor)
        {
            return ((ICoreExpressionVisitor<T, C>) visitor).visit(this, context);
        }
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(ValueVector groups, IExecutionContext context)
    {
        IExpression e = expression;
        if (e instanceof AliasExpression)
        {
            e = ((AliasExpression) expression).getExpression();
        }
        // A wrapped aggregate alias expression
        if (e instanceof IAggregateExpression)
        {
            return ((IAggregateExpression) e).eval(groups, context);
        }

        if (groups.type()
                .getType() != Type.TupleVector)
        {
            throw new IllegalArgumentException("Wrong type of input vector, expected tuple vector but got: " + groups.type());
        }

        final int size = groups.size();
        final ValueVector[] result = new ValueVector[size];

        for (int i = 0; i < size; i++)
        {
            TupleVector vector = (TupleVector) groups.getValue(i);
            result[i] = expression.eval(vector, context);
        }

        if (singleValue)
        {
            return new ValueVectorAdapter(row -> result[row], size, result[0].isNullable(), result[0].type())
            {
                @Override
                protected int getRow(int row)
                {
                    // Return the first row from each group since they are all the same
                    return 0;
                }
            };
        }

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.valueVector(expression.getType());
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public Object getValue(int row)
            {
                return result[row];
            }
        };
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
        else if (obj instanceof AggregateWrapperExpression)
        {
            AggregateWrapperExpression that = (AggregateWrapperExpression) obj;
            return expression.equals(that.expression)
                    && singleValue == that.singleValue
                    && internal == that.internal;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString();
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString();
    }
}
