package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;

/**
 * Filtered populated tuple vector. Works with an inner and outer tuplevector along with a filter. For each unique outer row a unique populated tuplevector is returned
 */
class PredicatedPopulatedTupleVector implements TupleVector
{
    private final TupleVector outer;
    private final TupleVector inner;
    private final ValueVector filter;
    private final Schema schema;
    private final int innerRowCount;
    private final int filterSize;
    private final int size;

    PredicatedPopulatedTupleVector(TupleVector outer, TupleVector inner, ValueVector filter, String populateAlias)
    {
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.filter = requireNonNull(filter, "filter");

        if (filter.size() != outer.getRowCount() * inner.getRowCount())
        {
            throw new IllegalArgumentException("Filter must equal the cartesian size of outer and inner");
        }
        else if (filter.type()
                .getType() != Type.Boolean)
        {
            throw new IllegalArgumentException("Filter must be of boolean type");
        }

        this.innerRowCount = inner.getRowCount();
        this.filterSize = filter.size();
        this.size = getOuterCardinality();
        this.schema = outer.getSchema()
                .populate(populateAlias, inner.getSchema());
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public int getRowCount()
    {
        return size;
    }

    @Override
    public ValueVector getColumn(int column)
    {
        // Outer access
        if (column < outer.getSchema()
                .getSize())
        {
            return new ValueVectorAdapter(outer.getColumn(column))
            {
                @Override
                public int size()
                {
                    return size;
                }

                @Override
                public int getRow(int row)
                {
                    return getOuterIndex(row, 0);
                }
            };
        }

        // Inner access => populated
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.tupleVector(inner.getSchema());
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                // We have equal amount of tuple vectors that we have outer rows
                return size;
            }

            @Override
            public Object getValue(final int outerRow)
            {
                // Find out cardinality of outer row and at which index the inner row starts
                int tinnerStartIndex = -1;
                int prevOuter = -1;
                int outerMatchCount = -1;
                int innerMatchCount = 0;

                for (int i = 0; i < filterSize; i++)
                {
                    if (!filter.getPredicateBoolean(i))
                    {
                        continue;
                    }

                    int outerIndex = i / innerRowCount;
                    if (prevOuter != outerIndex)
                    {
                        outerMatchCount++;
                    }

                    if (outerMatchCount == outerRow)
                    {
                        // Remember where the inner rows start
                        if (tinnerStartIndex == -1)
                        {
                            tinnerStartIndex = i;
                        }
                        innerMatchCount++;
                    }
                    // No need to search anymore when we passed the outer rows index
                    else if (outerMatchCount > outerRow)
                    {
                        break;
                    }

                    prevOuter = outerIndex;
                }

                final int innerStartIndex = tinnerStartIndex;
                final int innerRowCount = innerMatchCount;
                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return inner.getSchema();
                    }

                    @Override
                    public int getRowCount()
                    {
                        return innerRowCount;
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        return new ValueVectorAdapter(inner.getColumn(column))
                        {
                            @Override
                            public int size()
                            {
                                return innerRowCount;
                            }

                            @Override
                            protected int getRow(int row)
                            {
                                // TODO: This maybe needs some performance fixes. We start searching from start on each row
                                // To fix this an iterator might be added to ValueVector that can hold state
                                return getInnerIndex(row, innerStartIndex);
                            }
                        };
                    }

                    @Override
                    public String toString()
                    {
                        return "PredicatedPopulatedTupleVector";
                    }
                };
            }
        };
    }

    private int getOuterCardinality()
    {
        int count = 0;
        int prevOuter = -1;
        for (int i = 0; i < filterSize; i++)
        {
            if (!filter.getPredicateBoolean(i))
            {
                continue;
            }

            int outerIndex = i / innerRowCount;
            if (prevOuter != outerIndex)
            {
                count++;
            }
            prevOuter = outerIndex;
        }
        return count;
    }

    /**
     * Find next outer index of provided input row.
     * 
     * <pre>
     * 0, 1 <=> 0 0 1
     * If we have vectors and filter like this
     * outer    0 0 0 1 1 1
     * inner    0 0 1 0 0 1
     * filter   0 1 1 0 0 1
     * 
     * outer cardinality 2 (2 true outer indices)
     * Outer mapping
     *   row0: 0
     *   row1: 1
     * 
     * Inner mapping
     *   Outer row0
     *     row0: 1
     *     row1: 2
     *   Outer row1
     *     row0: 5
     * 
     * </pre>
     */
    private int getOuterIndex(int row, int startIndex)
    {
        int prevOuter = -1;
        int matchCount = -1;
        // Adjust fields when we start in the middle
        if (startIndex > 0)
        {
            prevOuter = row - 1;
            matchCount = row - 1;
        }
        for (int i = startIndex; i < filterSize; i++)
        {
            if (!filter.getPredicateBoolean(i))
            {
                continue;
            }

            int outerIndex = i / innerRowCount;

            // Count matches when we switch outer index
            if (prevOuter != outerIndex)
            {
                if (++matchCount == row)
                {
                    return outerIndex;
                }
            }
            prevOuter = outerIndex;
        }
        throw new NoSuchElementException();
    }

    private int getInnerIndex(int row, int startIndex)
    {
        // We can start counting matches to reach row number since startIndex points at the first
        int matchCount = -1;
        for (int i = startIndex; i < filterSize; i++)
        {
            if (!filter.getPredicateBoolean(i))
            {
                continue;
            }

            matchCount++;
            if (matchCount == row)
            {
                return i % innerRowCount;

            }
        }
        throw new NoSuchElementException();
    }
}
