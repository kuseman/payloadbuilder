package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

import it.unimi.dsi.fastutil.ints.IntHash.Strategy;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenCustomHashSet;

/** Index predicate used when creating index operators from catalogs */
class SeekPredicate implements ISeekPredicate
{
    /** Index used in this predicate */
    private final Index index;
    /**
     * Columns used in this predicate. If the index is of ALL type this list will contain the found columns to use
     */
    private final List<String> indexColumns;
    private final List<IExpression> expressions;

    SeekPredicate(Index index, List<String> indexColumns, List<IExpression> expressions)
    {
        this.index = requireNonNull(index, "index");
        this.indexColumns = unmodifiableList(requireNonNull(indexColumns, "indexColumns"));
        this.expressions = requireNonNull(expressions);

        if (indexColumns.size() != expressions.size())
        {
            throw new IllegalArgumentException("Index columns and expressions must equal in size");
        }
    }

    @Override
    public Index getIndex()
    {
        return index;
    }

    @Override
    public List<String> getIndexColumns()
    {
        return indexColumns;
    }

    @Override
    public List<ISeekKey> getSeekKeys(IExecutionContext context)
    {
        TupleVector tupleVector = ((StatementContext) context.getStatementContext()).getIndexSeekTupleVector();

        if (tupleVector == null)
        {
            throw new IllegalArgumentException("Expected an index seek tuple vector in context");
        }

        int size = expressions.size();
        final ValueVector[] vectors = new ValueVector[size];

        for (int i = 0; i < size; i++)
        {
            vectors[i] = expressions.get(i)
                    .eval(tupleVector, context);
        }

        int rowCount = tupleVector.getRowCount();
        // We use a linked hash set here to keep the order from input
        IntLinkedOpenCustomHashSet set = new IntLinkedOpenCustomHashSet(rowCount, new Strategy()
        {
            @Override
            public int hashCode(int e)
            {
                return VectorUtils.hash(vectors, e);
            }

            @Override
            public boolean equals(int a, int b)
            {
                return VectorUtils.equals(vectors, a, b);
            }
        });

        for (int i = 0; i < rowCount; i++)
        {
            // If any of the vectors has a null value it should be ignored since
            // seek predicates usage at the time of writing should not be used with nulls.
            // Ie. predicate: 'on a.col = b.col' where b.col has a seek predicate
            // a null value can never yield a match
            boolean hasNull = false;
            for (int j = 0; j < size; j++)
            {
                if (vectors[j].isNull(i))
                {
                    hasNull = true;
                    break;
                }
            }
            if (hasNull)
            {
                continue;
            }
            set.add(i);
        }

        final int[] ordinals = set.toIntArray();
        final int resultSize = ordinals.length;
        List<ISeekKey> seekKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            final ValueVector keyVector = new ValueVectorAdapter(vectors[i])
            {
                @Override
                public int size()
                {
                    return resultSize;
                }

                @Override
                protected int getRow(int row)
                {
                    return ordinals[row];
                }
            };
            seekKeys.add(new ISeekKey()
            {
                @Override
                public ValueVector getValue()
                {
                    return keyVector;
                }

                @Override
                public SeekType getType()
                {
                    // Only EQ supported for now
                    return SeekType.EQ;
                }
            });
        }
        return seekKeys;
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
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
        else if (obj instanceof SeekPredicate)
        {
            SeekPredicate that = (SeekPredicate) obj;
            return index.equals(that.index)
                    && indexColumns.equals(that.indexColumns)
                    && expressions.equals(that.expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return IntStream.range(0, indexColumns.size())
                .mapToObj(i -> indexColumns.get(i) + " = " + expressions.get(i))
                .collect(joining(", "));
    }
}
