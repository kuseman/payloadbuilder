package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

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
    // private final Type type = Type.KEY_LOOKUP;

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

    // @Override
    // public Type getType()
    // {
    // return type;
    // }

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
        TupleVector tupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();

        if (tupleVector == null)
        {
            throw new IllegalArgumentException("Expected a tuple vector in context");
        }

        List<ISeekKey> keys = new ArrayList<>(expressions.size());

        for (final IExpression expression : expressions)
        {
            keys.add(new ISeekKey()
            {
                final ValueVector value = expression.eval(tupleVector, context);

                @Override
                public ValueVector getValue()
                {
                    return value;
                }

                @Override
                public SeekType getType()
                {
                    // Only supports EQ at this time
                    return SeekType.EQ;
                }
            });
        }

        return keys;
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

    // /**
    // * Return outervalues used by this index predicate. Only applicable when have a KEY_LOOKUP index predicate
    // */
    // @Override
    // public Iterator<IOrdinalValues> getOuterValuesIterator(IExecutionContext context)
    // {
    // if (type != Type.KEY_LOOKUP)
    // {
    // throw new IllegalArgumentException("This index predicate is not of " + Type.KEY_LOOKUP + " type");
    // }
    //
    // return ((StatementContext) context.getStatementContext()).getOuterOrdinalValues();
    // }
}
