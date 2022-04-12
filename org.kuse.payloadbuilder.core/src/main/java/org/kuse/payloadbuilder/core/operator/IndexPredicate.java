package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Index;

/** Index predicate used when creating index operators from catalogs */
public class IndexPredicate
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

    public Index getIndex()
    {
        return index;
    }

    public List<String> getIndexColumns()
    {
        return indexColumns;
    }

    /**
     * Return outervalues used by this index predicate. Only applicable when have a KEY_LOOKUP index predicate
     */
    public Iterator<IOrdinalValuesFactory.IOrdinalValues> getOuterValuesIterator(ExecutionContext context)
    {
        if (type != Type.KEY_LOOKUP)
        {
            throw new IllegalArgumentException("This index predicate is not of " + Type.KEY_LOOKUP + " type");
        }

        return context.getStatementContext().getOuterOrdinalValues();
    }

    /** Type of predicate */
    public enum Type
    {
        /**
         * <pre>
         * Index is used to lookup entries by columns in index. Can by used by join operators to retrieve inner tuples from values in the outer tuples
         */
        KEY_LOOKUP
    }
}
