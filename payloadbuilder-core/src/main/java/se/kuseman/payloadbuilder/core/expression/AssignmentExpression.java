package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

/** Assignment expression. Used in projections to assign variables from projection result */
public class AssignmentExpression implements IExpression
{
    private final IExpression expression;
    private final QualifiedName variable;

    public AssignmentExpression(IExpression expression, QualifiedName qname)
    {
        this.expression = requireNonNull(expression, "expression");
        this.variable = requireNonNull(qname, "qname").toLowerCase();
    }

    public IExpression getExpression()
    {
        return expression;
    }

    public QualifiedName getVariable()
    {
        return variable;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        int size = input.getRowCount();
        for (int i = 0; i < size; i++)
        {
            ValueVector value = expression.eval(new RowTupleVector(input, i), context);
            ((ExecutionContext) context).setVariable(variable, value);
        }

        // These expressions is never used so return an empty value vector
        return ValueVector.literalNull(ResolvedType.of(Type.Any), 0);
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
        private TupleVector wrapped;
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
