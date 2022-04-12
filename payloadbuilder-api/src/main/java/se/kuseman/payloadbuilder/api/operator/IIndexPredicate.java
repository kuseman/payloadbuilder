package se.kuseman.payloadbuilder.api.operator;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Index;

/** Index predicate used when creating index operators from catalogs */
public interface IIndexPredicate
{
    /** Return the index for this predicate */
    Index getIndex();

    /** Get type of index predicate */
    Type getType();

    /** Return the used columns for this predicate */
    List<String> getIndexColumns();

    /** Return an outer values iterator. Only applicable when {#getType()} is {@link Type#KEY_LOOKUP} */
    Iterator<IOrdinalValues> getOuterValuesIterator(IExecutionContext context);

    /** Type of predicate */
    public enum Type
    {
        /**
         * Index is used to lookup entries by columns in index. Can by used by join operators to retrieve inner tuples from values in the outer tuples
         */
        KEY_LOOKUP
    }
}
