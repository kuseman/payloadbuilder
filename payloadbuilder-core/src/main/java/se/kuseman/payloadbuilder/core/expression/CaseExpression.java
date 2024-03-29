package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** CASE WHEN expression */
public class CaseExpression implements ICaseExpression
{
    private final List<WhenClause> whenClauses;
    private final IExpression elseExpression;

    public CaseExpression(List<WhenClause> whenClauses, IExpression elseExpression)
    {
        this.whenClauses = requireNonNull(whenClauses, "whenClauses");
        this.elseExpression = elseExpression;
    }

    @Override
    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    @Override
    public IExpression getElseExpression()
    {
        return elseExpression;
    }

    @Override
    public List<IExpression> getChildren()
    {
        List<IExpression> children = new ArrayList<>((whenClauses.size() * 2) + 1);
        int size = whenClauses.size();
        for (int i = 0; i < size; i++)
        {
            children.add(whenClauses.get(i)
                    .getCondition());
            children.add(whenClauses.get(i)
                    .getResult());
        }

        if (elseExpression != null)
        {
            children.add(elseExpression);
        }
        return children;
    }

    @Override
    public ResolvedType getType()
    {
        // Find the highest precedence return type
        ResolvedType type = whenClauses.get(0)
                .getResult()
                .getType();
        int size = whenClauses.size();
        for (int i = 1; i < size; i++)
        {
            ResolvedType t = whenClauses.get(i)
                    .getResult()
                    .getType();
            if (t.getType()
                    .getPrecedence() > type.getType()
                            .getPrecedence())
            {
                type = t;
            }
        }

        if (elseExpression != null)
        {
            ResolvedType t = elseExpression.getType();
            if (t.getType()
                    .getPrecedence() > type.getType()
                            .getPrecedence())
            {
                type = t;
            }
        }

        return type;
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ResolvedType type = getType();
        int size = whenClauses.size();
        ValueVector[] whenConditions = new ValueVector[size];
        ValueVector[] whenResults = new ValueVector[size];
        ValueVector elseResult = null;

        int rowCount = input.getRowCount();
        IValueVectorBuilder builder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(type, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            boolean allFalse = true;
            for (int j = 0; j < size; j++)
            {
                if (whenConditions[j] == null)
                {
                    whenConditions[j] = whenClauses.get(j)
                            .getCondition()
                            .eval(input, context);
                }
                if (whenConditions[j].getPredicateBoolean(i))
                {
                    if (whenResults[j] == null)
                    {
                        whenResults[j] = whenClauses.get(j)
                                .getResult()
                                .eval(input, context);
                    }
                    allFalse = false;
                    builder.put(whenResults[j], i);
                    break;
                }
            }

            if (allFalse)
            {
                if (elseExpression != null)
                {
                    if (elseResult == null)
                    {
                        elseResult = elseExpression.eval(input, context);
                    }
                    builder.put(elseResult, i);
                }
                else
                {
                    builder.putNull();
                }
            }
        }

        return builder.build();
    }

    @Override
    public int hashCode()
    {
        return whenClauses.hashCode();
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
        else if (obj instanceof CaseExpression)
        {
            CaseExpression that = (CaseExpression) obj;
            return whenClauses.equals(that.whenClauses)
                    && Objects.equals(elseExpression, that.elseExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        /*
         * CASE WHEN ..... WHEN ..... ELSE .... END
         *
         */
        return "CASE" + whenClauses.stream()
                .map(w -> " WHEN " + w.getCondition() + " THEN " + w.getResult())
                .collect(joining(" "))
               + (elseExpression != null ? (" ELSE " + elseExpression)
                       : "")
               + " END";
    }
}
