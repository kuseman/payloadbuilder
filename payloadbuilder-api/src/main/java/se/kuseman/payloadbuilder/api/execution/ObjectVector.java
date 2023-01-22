package se.kuseman.payloadbuilder.api.execution;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/**
 * Definition of a object vector representing the type {@link Column.Type#Object}. Consist of a {@link Schema} and a collection of {@link ValueVector}. all with row count 1.
 */
public interface ObjectVector
{
    public static final ObjectVector EMPTY = new ObjectVector()
    {
        @Override
        public ValueVector getValue(int ordinal)
        {
            throw new IllegalArgumentException("Empty object vector has no values");
        }

        @Override
        public Schema getSchema()
        {
            return Schema.EMPTY;
        }
    };

    /**
     * Return the row to use in {@link ValueVector}'s to access the objects values. This is used when sharing vectors among several objects to denote which underlying row to use for this instance.
     */
    default int getRow()
    {
        return 0;
    }

    /** Return schema for the vector */
    Schema getSchema();

    /** Get the value of provided ordinal */
    ValueVector getValue(int ordinal);

    /** Wrap a {@link TupleVector} creating a {@link ObjectVector} for row 0 */
    static ObjectVector wrap(TupleVector tupleVector)
    {
        return wrap(tupleVector, 0);
    }

    /** Wrap a {@link TupleVector} creating a {@link ObjectVector} for provided row */
    static ObjectVector wrap(final TupleVector tupleVector, final int row)
    {
        if (row < 0
                || row >= tupleVector.getRowCount())
        {
            throw new IllegalArgumentException("Provided row is out of bounds");
        }
        return new ObjectVector()
        {
            @Override
            public int getRow()
            {
                return row;
            }

            @Override
            public ValueVector getValue(int ordinal)
            {
                return tupleVector.getColumn(ordinal);
            }

            @Override
            public Schema getSchema()
            {
                return tupleVector.getSchema();
            }
        };
    }
}
