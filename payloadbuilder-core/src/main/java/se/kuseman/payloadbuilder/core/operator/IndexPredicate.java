package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;

/** Index predicate used when creating index operators from catalogs */
public class IndexPredicate implements IIndexPredicate
{
    /** Index used in this predicate */
    private final Index index;
    /**
     * Columns used in this predicate. If the index is of ALL type this list will contain the found columns to use
     */
    private final List<String> indexColumns;
    private final Type type = Type.KEY_LOOKUP;

    IndexPredicate(Index index, List<String> indexColumns)
    {
        this.index = requireNonNull(index, "index");
        this.indexColumns = unmodifiableList(requireNonNull(indexColumns, "indexColumns"));
    }

    @Override
    public Type getType()
    {
        return type;
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

    /**
     * Return outervalues used by this index predicate. Only applicable when have a KEY_LOOKUP index predicate
     */
    @Override
    public Iterator<IOrdinalValues> getOuterValuesIterator(IExecutionContext context)
    {
        if (type != Type.KEY_LOOKUP)
        {
            throw new IllegalArgumentException("This index predicate is not of " + Type.KEY_LOOKUP + " type");
        }

        return ((StatementContext) context.getStatementContext()).getOuterOrdinalValues();
    }
}
