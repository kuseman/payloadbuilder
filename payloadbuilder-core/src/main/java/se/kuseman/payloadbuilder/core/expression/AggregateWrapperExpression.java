package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

/** An aggregate expression that wrapps a ordinary expression and turns it into an aggregate result */
public class AggregateWrapperExpression implements IAggregateExpression, HasAlias, HasTableSourceReference
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
    public TableSourceReference getTableSourceReference()
    {
        if (expression instanceof HasTableSourceReference htsr)
        {
            return htsr.getTableSourceReference();
        }
        return null;
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
        private final ObjectList<ValueVector> result = new ObjectArrayList<>();

        ExpressionAggregator(boolean singleValue, IExpression expression)
        {
            this.singleValue = singleValue;
            this.expression = expression;
        }

        @Override
        public void appendGroup(TupleVector groupData, IExecutionContext context)
        {
            ValueVector groupTables = groupData.getColumn(0);
            ValueVector groupIds = groupData.getColumn(1);

            int groupCount = groupData.getRowCount();

            for (int i = 0; i < groupCount; i++)
            {
                int group = groupIds.getInt(i);
                result.size(Math.max(result.size(), group + 1));

                TupleVector vector = groupTables.getTable(i);
                if (vector.getRowCount() == 0)
                {
                    continue;
                }

                ValueVector groupResult = expression.eval(vector, context);
                result.set(group, groupResult);
            }
        }

        @Override
        public ValueVector combine(IExecutionContext context)
        {
            final int size = result.size();
            // Pick first row from all groups
            if (singleValue)
            {
                IValueVectorBuilder builder = context.getVectorBuilderFactory()
                        .getValueVectorBuilder(result.get(0)
                                .type(), size);
                for (int i = 0; i < size; i++)
                {
                    builder.put(result.get(i), 0);
                }
                return builder.build();
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
                    return result.get(row);
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
