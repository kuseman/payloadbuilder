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
 * A predicated tuple vector that acts as result from a predicate evaluation. This tuple vector is the result of a join (either inner or left)
 */
public class PredicatedTupleVector implements TupleVector
{
    private final TupleVector target;
    private final Schema schema;
    private final ValueVector filter;
    private final int size;
    private final int filterSize;
    private final int targetColumnSize;

    public PredicatedTupleVector(TupleVector target, ValueVector filter)
    {
        this(target, target.getSchema(), filter);
    }

    PredicatedTupleVector(TupleVector target, Schema schema, ValueVector filter)
    {
        this.target = requireNonNull(target, "target");
        this.schema = requireNonNull(schema, "schema");
        this.filter = requireNonNull(filter, "filter");
        this.filterSize = filter.size();
        if (filter.type()
                .getType() != Type.Boolean)
        {
            throw new IllegalArgumentException("Filter must be of boolean type");
        }
        this.size = filter.getCardinality();
        this.targetColumnSize = target.getSchema()
                .getColumns()
                .size();
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
        // This tuple vector is executed with a different schema than the actual target vector (left join)
        // so return a null literal in those cases
        if (column >= targetColumnSize)
        {
            ResolvedType type = ResolvedType.of(Type.Any);
            if (column < schema.getColumns()
                    .size())
            {
                type = schema.getColumns()
                        .get(column)
                        .getType();
            }

            return ValueVector.literalNull(type, size);
        }

        return new ValueVectorAdapter(target.getColumn(column))
        {
            private int nextRow;
            private int nextIndex;

            @Override
            public int size()
            {
                return size;
            }

            @Override
            protected int getRow(int row)
            {
                boolean seqAcc = row == nextRow;
                int index = getIndex(row, seqAcc ? nextIndex
                        : 0);
                nextRow = row + 1;
                nextIndex = index + 1;
                return index;
            };
        };
    }

    private int getIndex(int row, int startIndex)
    {
        int matchCount = -1;
        for (int i = startIndex; i < filterSize; i++)
        {
            if (filter.getPredicateBoolean(i))
            {
                if (startIndex > 0
                        || ++matchCount == row)
                {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public String toString()
    {
        return "PredicatedTupleVector";
    }
}
