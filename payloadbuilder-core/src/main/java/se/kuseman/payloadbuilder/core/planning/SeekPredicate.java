package se.kuseman.payloadbuilder.core.planning;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.IndexType;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.SelectedValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

import it.unimi.dsi.fastutil.ints.IntHash.Strategy;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenCustomHashSet;

/** Index predicate used on {@link IndexType#SEEK_EQ} index types when creating index operators from catalogs */
class SeekPredicate implements ISeekPredicate
{
    /** Index used in this predicate */
    private final Index index;
    /** Columns used in index */
    private final List<String> indexColumns;
    private final List<SeekPredicateItem> predicateItems;

    /**
     * If this seek predicate is used as a push down predicate then this flag is set. ie. 'where a.col in (1,2,3)' and there is an index on 'col'
     */
    private final boolean isPushDown;

    SeekPredicate(Index index, List<SeekPredicateItem> predicateItems)
    {
        this(index, predicateItems, false);
    }

    SeekPredicate(Index index, List<SeekPredicateItem> predicateItems, boolean isPushDown)
    {
        this.index = requireNonNull(index, "index");
        this.predicateItems = requireNonNull(predicateItems, "predicateItems");
        this.isPushDown = isPushDown;

        if (predicateItems.isEmpty())
        {
            throw new IllegalArgumentException("Predicate items cannot be empty");
        }

        this.indexColumns = predicateItems.stream()
                .map(i -> i.column)
                .toList();
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
        if (isPushDown)
        {
            return getPushDownSeekKeys(context);
        }

        TupleVector tupleVector = ((StatementContext) context.getStatementContext()).getIndexSeekTupleVector();

        if (tupleVector == null)
        {
            throw new IllegalArgumentException("Expected an index seek tuple vector in context");
        }
        int size = predicateItems.size();
        ValueVector[] vectors = new ValueVector[size];

        for (int i = 0; i < size; i++)
        {
            // In non pushdown mode there are only single value expressions
            vectors[i] = predicateItems.get(i).valueExpressions.get(0)
                    .eval(tupleVector, context);
        }

        ValueVector selection = getUniqueRowSelection(vectors, tupleVector.getRowCount());
        List<ISeekKey> seekKeys = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ValueVector keyVector = SelectedValueVector.select(vectors[i], selection);
            seekKeys.add(new ISeekKey()
            {
                @Override
                public ValueVector getValue()
                {
                    return keyVector;
                }
            });
        }
        return seekKeys;
    }

    private ValueVector getUniqueRowSelection(ValueVector[] vectors, int rowCount)
    {
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

        int size = vectors.length;
        rows: for (int i = 0; i < rowCount; i++)
        {
            // If any of the vectors has a null value it should be ignored since
            // seek predicates usage at the time of writing should not be used with nulls.
            // Ie. predicate: 'on a.col = b.col' where b.col has a seek predicate
            // a null value can never yield a match
            for (int j = 0; j < size; j++)
            {
                if (vectors[j].isNull(i))
                {
                    break rows;
                }
            }
            set.add(i);
        }

        int[] ordinals = set.toIntArray();
        return VectorUtils.convertToSelectionVector(ordinals, ordinals.length);
    }

    private List<ISeekKey> getPushDownSeekKeys(IExecutionContext context)
    {
        int itemCount = predicateItems.size();
        List<ISeekKey> seekKeys = new ArrayList<>(itemCount);

        /*
         * single column single expression => single seek key one value a.col = 10
         *
         * single column multi expressions => single seek key multi value a.col in (10,20,30)
         *
         * multi column single expression => multi seek key one value [a.col = 10 and a.col2 = 20]
         */

        for (int i = 0; i < itemCount; i++)
        {
            SeekPredicateItem item = predicateItems.get(i);
            if (item.valueExpressions.size() == 1)
            {
                ValueVector vector = item.valueExpressions.get(0)
                        .eval(context);
                seekKeys.add(new ISeekKey()
                {
                    @Override
                    public ValueVector getValue()
                    {
                        return vector;
                    }
                });
            }
            else
            {
                // Multi expression => IN or any other multi argument expression that can be used as seek predicate value
                int size = item.valueExpressions.size();
                ResolvedType type = item.valueExpressions.get(0)
                        .getType();
                for (int j = 1; j < size; j++)
                {
                    ResolvedType tmpType = item.valueExpressions.get(j)
                            .getType();
                    if (tmpType.getType()
                            .getPrecedence() > type.getType()
                                    .getPrecedence())
                    {
                        type = tmpType;
                    }
                }
                MutableValueVector resultVector = context.getVectorFactory()
                        .getMutableVector(type, size);
                for (int j = 0; j < size; j++)
                {
                    ValueVector vector = item.valueExpressions.get(j)
                            .eval(context);
                    resultVector.copy(j, vector, 0);
                }
                seekKeys.add(new ISeekKey()
                {

                    @Override
                    public ValueVector getValue()
                    {
                        return resultVector;
                    }

                });
            }
        }
        return seekKeys;
    }

    @Override
    public int hashCode()
    {
        return predicateItems.hashCode();
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
        else if (obj instanceof SeekPredicate that)
        {
            return index.equals(that.index)
                    && predicateItems.equals(that.predicateItems)
                    && isPushDown == that.isPushDown;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return IntStream.range(0, predicateItems.size())
                .mapToObj(i -> predicateItems.get(i).column + " = " + predicateItems.get(i).valueExpressions)
                .collect(joining(", "));
    }

    /**
     * Record with a seek item. A column with it's expression along with the list of value expressions used to calculate the value.
     *
     * <pre>
     * Example:
     *   - Join
     *     on a.indexCol = b.col
     *
     *     Item: column = 'indexCol', columnExpression = a.indexCol, valueExpressions = [ b.col ]
     *
     *   - Push down
     *     where a.indexCol = 10
     *
     *     Item: column = 'indexCol', columnExpression = a.indexCol, valueExpressions = [ 10 ]
     *
     *   - Push down
     *     where a.indexCol in (10,20,30)
     *
     *     Item: column = 'indexCol', columnExpression = a.indexCol, valueExpressions = [ 10,20,30 ]
     * </pre>
     */
    record SeekPredicateItem(String column, IExpression columnExpression, List<IExpression> valueExpressions)
    {
    }
}
