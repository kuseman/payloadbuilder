package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

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
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        ResolvedType type = getType();
        int size = whenClauses.size();
        int rowCount = selection.size();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(type, rowCount);

        // Start with all rows as unmatched
        IntList nonMatchedRows = new IntArrayList(rowCount);
        IntList nonMatchedOrdinals = new IntArrayList(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            nonMatchedRows.add(selection.getInt(i));
            nonMatchedOrdinals.add(i);
        }
        IntList matchedRows = new IntArrayList();
        IntList matchedOrdinals = new IntArrayList();

        ValueVector nonMatchedSelection = VectorUtils.convertToSelectionVector(nonMatchedRows);
        ValueVector matchedSelection = VectorUtils.convertToSelectionVector(matchedRows);

        for (int j = 0; j < size; j++)
        {
            WhenClause whenClause = whenClauses.get(j);

            // Eval the condition for the non matched rows
            ValueVector condition = whenClause.getCondition()
                    .eval(input, nonMatchedSelection, context);

            // Assemble current matched rows and map rows to result vector index
            int currentSize = nonMatchedRows.size();
            matchedRows.clear();
            matchedOrdinals.clear();
            for (int i = 0; i < currentSize; i++)
            {
                if (condition.getPredicateBoolean(i))
                {
                    int row = nonMatchedRows.getInt(i);
                    matchedRows.add(row);
                    matchedOrdinals.add(nonMatchedOrdinals.getInt(i));
                }
            }

            if (matchedRows.isEmpty())
            {
                continue;
            }

            // Remove all the matched rows from the unmatched list
            // these shouldn't be evaluated again
            nonMatchedRows.removeAll(matchedRows);
            nonMatchedOrdinals.removeAll(matchedOrdinals);

            // Eval the result with the matched rows
            ValueVector result = whenClause.getResult()
                    .eval(input, matchedSelection, context);
            currentSize = result.size();
            for (int i = 0; i < currentSize; i++)
            {
                int ordinal = matchedOrdinals.getInt(i);
                resultVector.copy(ordinal, result, i);
            }
        }

        // Evaluate else or set null in rows that wasn't processed
        if (!nonMatchedRows.isEmpty())
        {
            size = nonMatchedRows.size();
            ValueVector result = elseExpression != null ? elseExpression.eval(input, nonMatchedSelection, context)
                    : ValueVector.literalNull(ResolvedType.of(Type.Any), size);
            for (int i = 0; i < size; i++)
            {
                int ordinal = nonMatchedOrdinals.getInt(i);
                resultVector.copy(ordinal, result, i);
            }
        }
        return resultVector;
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
