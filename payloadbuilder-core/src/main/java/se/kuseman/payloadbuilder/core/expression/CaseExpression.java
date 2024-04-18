package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
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
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ResolvedType type = getType();
        int size = whenClauses.size();
        ValueVector[] whenResults = new ValueVector[size];
        IntList[] whenMatchedRows = new IntArrayList[size];
        ValueVector elseResult = null;

        int rowCount = input.getRowCount();
        IValueVectorBuilder builder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(type, rowCount);

        Int2IntMap rowToResultVector = new Int2IntOpenHashMap(rowCount * 4 / 3 + 1)
        {
            {
                defRetValue = -1;
            }
        };

        // Start with all rows as unmatched
        IntList nonMatchedRows = new IntArrayList(IntStream.range(0, rowCount)
                .toArray());
        RowsTupleVector nonMatchedVector = new RowsTupleVector(input, nonMatchedRows);

        for (int j = 0; j < size; j++)
        {
            WhenClause whenClause = whenClauses.get(j);

            // Eval the condition for the current non matched rows
            ValueVector condition = whenClause.getCondition()
                    .eval(nonMatchedVector, context);

            // Assemble current matched rows and map rows to result vector index
            int currentSize = nonMatchedRows.size();
            IntList currentMatchedRows = null;
            for (int i = 0; i < currentSize; i++)
            {
                if (condition.getPredicateBoolean(i))
                {
                    int row = nonMatchedRows.getInt(i);
                    if (currentMatchedRows == null)
                    {
                        currentMatchedRows = new IntArrayList(currentSize);
                        whenMatchedRows[j] = currentMatchedRows;
                    }
                    currentMatchedRows.add(row);
                    rowToResultVector.put(row, j);
                }
            }

            if (currentMatchedRows == null)
            {
                continue;
            }

            nonMatchedRows.removeAll(currentMatchedRows);

            // Eval the result with the matched rows
            RowsTupleVector evalVector = new RowsTupleVector(input, currentMatchedRows);
            whenResults[j] = whenClause.getResult()
                    .eval(evalVector, context);
        }

        if (elseExpression != null
                && !nonMatchedRows.isEmpty())
        {
            elseResult = elseExpression.eval(nonMatchedVector, context);
        }

        // Finally assemble result
        for (int i = 0; i < rowCount; i++)
        {
            int index = rowToResultVector.get(i);
            IntList rows = null;
            ValueVector result = null;
            if (index < 0)
            {
                rows = elseResult != null ? nonMatchedRows
                        : null;
                result = elseResult;
            }
            else
            {
                rows = whenMatchedRows[index];
                result = whenResults[index];
            }

            if (rows == null)
            {
                builder.putNull();
            }
            else
            {
                builder.put(result, rows.indexOf(i));
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

    static class RowsTupleVector implements TupleVector
    {
        private final TupleVector vector;
        private final IntList rows;

        RowsTupleVector(TupleVector vector, IntList rows)
        {
            this.vector = vector;
            this.rows = rows;
        }

        @Override
        public int getRowCount()
        {
            return rows.size();
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return new ValueVectorAdapter(vector.getColumn(column))
            {
                @Override
                protected int getRow(int row)
                {
                    return rows.getInt(row);
                }

                @Override
                public int size()
                {
                    return rows.size();
                }
            };
        }

        @Override
        public Schema getSchema()
        {
            return vector.getSchema();
        }
    }
}
