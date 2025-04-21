package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

/** Assignment expression. Used in projections to assign variables from projection result */
public class AssignmentExpression implements IExpression
{
    private final IExpression expression;
    private final String variable;

    public AssignmentExpression(IExpression expression, String variable)
    {
        this.expression = requireNonNull(expression, "expression");
        this.variable = requireNonNull(variable, "variable").toLowerCase();
    }

    public IExpression getExpression()
    {
        return expression;
    }

    public String getVariable()
    {
        return variable;
    }

    /**
     * Used when aggregating with assignment's then we set the downstream expression's aggregate result into the variable.
     */
    public IAggregator createAggregator()
    {
        final IAggregator wrapper = ((IAggregateExpression) expression).createAggregator();
        return new IAggregator()
        {
            ValueVector combinedResult;

            @Override
            public ValueVector combine(IExecutionContext context)
            {
                return ValueVector.literalNull(getType(), 0);
            }

            @Override
            public void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context)
            {
                ValueVector var = ((ExecutionContext) context).getVariableValue(variable);
                if (var == null
                        || !(var instanceof MutableValueVector))
                {
                    ValueVector old = var;

                    var = context.getVectorFactory()
                            .getMutableVector(expression.getType(), 1);

                    // Copy any previous value
                    if (old != null)
                    {
                        ((MutableValueVector) var).copy(0, old, 0, 1);
                    }

                    ((ExecutionContext) context).setVariable(variable, var);
                }

                // Need to evaluate each group individually and extract results
                // in between to be able to have
                // assignment expressions with side effects ie. @max = max(@max + col1)
                // This is not very effective since the downstream aggregator might allocate stuff
                // upon combine, but it's the easiest way atm.
                // NOTE! To fix this the IAggregate interface should only have appendGroup method
                // and let that return intermediate results after each invocation
                int size = groupIds.size();
                for (int i = 0; i < size; i++)
                {
                    int groupId = groupIds.getInt(i);
                    ValueVector tempGroupIds = ValueVector.literalInt(groupId, 1);
                    ValueVector tempSelection = ValueVector.literalArray(selections.getArray(i), 1);

                    wrapper.appendGroup(input, tempGroupIds, tempSelection, context);

                    combinedResult = wrapper.combine(context);
                    ((MutableValueVector) var).copy(0, combinedResult, groupId, 1);
                }
            }
        };
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        int size = input.getRowCount();
        MutableValueVector result = context.getVectorFactory()
                .getMutableVector(expression.getType(), 1);
        // We need set the result vector into context before we loop all rows
        // since we might have expressions that references the variable in question
        // ie. @var = @var + 1
        ((ExecutionContext) context).setVariable(variable, result);
        for (int i = 0; i < size; i++)
        {
            ValueVector value = expression.eval(new RowTupleVector(input, i), context);
            // Copy the intermediate result value
            result.copy(0, value, 0, 1);
        }

        // These expressions is never used so return an empty value vector
        return ValueVector.literalNull(expression.getType(), 0);
    }

    @Override
    public boolean isConstant()
    {
        // An assignment expression is not constant even if the wrapped expression is
        return false;
    }

    @Override
    public ResolvedType getType()
    {
        return expression.getType();
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
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
        else if (obj instanceof AssignmentExpression)
        {
            AssignmentExpression that = (AssignmentExpression) obj;
            return expression.equals(that.expression)
                    && variable.equals(that.variable);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "@" + variable + " = " + expression;
    }

    @Override
    public String toVerboseString()
    {
        return "@" + variable + " = " + expression.toVerboseString();
    }

    /**
     * Tuple vector that acts as a single row since assignment expressions needs to be evaluated row by row when there are cyclic dependencies ala "var = var + 1"
     */
    private static class RowTupleVector implements TupleVector
    {
        private final TupleVector wrapped;
        private final int row;

        RowTupleVector(TupleVector vector, int row)
        {
            this.wrapped = vector;
            this.row = row;
        }

        @Override
        public int getRowCount()
        {
            return 1;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            final ValueVector vector = wrapped.getColumn(column);
            return new ValueVectorAdapter(vector)
            {
                @Override
                public int size()
                {
                    return 1;
                }

                @Override
                protected int getRow(int row)
                {
                    return RowTupleVector.this.row;
                }
            };
        }

        @Override
        public Schema getSchema()
        {
            return wrapped.getSchema();
        }
    }
}
