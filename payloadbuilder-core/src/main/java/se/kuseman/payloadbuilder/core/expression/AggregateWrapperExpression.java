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
import se.kuseman.payloadbuilder.api.expression.IAggregator;
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
            return e.getType();
        }

        // We return a value vector if it's not a single value
        return singleValue ? expression.getType()
                : ResolvedType.array(expression.getType());
    }

    @Override
    public ResolvedType getAggregateType()
    {
        return getType();
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
    public IAggregator createAggregator()
    {
        IExpression e = expression;
        if (e instanceof AliasExpression)
        {
            e = ((AliasExpression) expression).getExpression();
        }

        if (e instanceof AssignmentExpression ase)
        {
            return ase.createAggregator();
        }

        // A wrapped aggregate alias expression
        if (e instanceof IAggregateExpression)
        {
            return ((IAggregateExpression) e).createAggregator();
        }

        return new ExpressionAggregator(singleValue, e);
    }

    private static class ExpressionAggregator implements IAggregator
    {
        private final boolean singleValue;
        private final IExpression expression;
        private MutableValueVector result;

        ExpressionAggregator(boolean singleValue, IExpression expression)
        {
            this.singleValue = singleValue;
            this.expression = expression;
        }

        @Override
        public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
        {
            int groupCount = groupIds.size();

            if (result == null)
            {
                result = context.getVectorFactory()
                        .getMutableVector(ResolvedType.of(Type.Any), groupCount);
            }

            for (int i = 0; i < groupCount; i++)
            {
                int groupId = groupIds.getInt(i);
                ValueVector selection = selections.getArray(i);
                if (result.size() < groupId)
                {
                    result.setNull(groupId);
                }
                if (selection.size() == 0)
                {
                    continue;
                }

                ValueVector groupResult = expression.eval(input, selection, context);
                MutableValueVector groupResultCopy = context.getVectorFactory()
                        .getMutableVector(groupResult.type(), groupResult.size());
                groupResultCopy.copy(0, groupResult);
                result.setAny(groupId, groupResultCopy);
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            final int size = result.size();
            // Pick first row from all groups
            if (singleValue)
            {
                ValueVector groupResult = (ValueVector) result.valueAsObject(0);

                MutableValueVector resultVector = context.getVectorFactory()
                        .getMutableVector(groupResult.type(), size);
                for (int i = 0; i < size; i++)
                {
                    groupResult = (ValueVector) result.valueAsObject(i);
                    resultVector.copy(i, groupResult, 0);
                }

                return resultVector;
            }

            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.array(expression.getType());
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
                public ValueVector getArray(int row)
                {
                    return (ValueVector) result.valueAsObject(row);
                }
            };
        }
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
