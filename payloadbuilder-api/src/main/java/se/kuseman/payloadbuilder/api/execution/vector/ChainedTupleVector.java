package se.kuseman.payloadbuilder.api.execution.vector;

import java.util.Arrays;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A tuple vector that chains a list of other tuple vectros, exposing those as a single vector */
public class ChainedTupleVector implements TupleVector
{
    private final Schema schema;
    private final List<TupleVector> vectors;
    private final int rowCount;
    /** Ordinals for where row counts starts in each nested tuple vector */
    private final int[] ordinals;
    private final ChainedValueVector[] columns;

    private ChainedTupleVector(Schema schema, List<TupleVector> vectors, int rowCount, int[] ordinals)
    {
        this.schema = schema;
        this.vectors = vectors;
        this.rowCount = rowCount;
        this.ordinals = ordinals;
        this.columns = new ChainedValueVector[schema.getSize()];
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public ValueVector getColumn(int column)
    {
        ChainedValueVector col = columns[column];
        if (col == null)
        {
            col = new ChainedValueVector(schema.getColumns()
                    .get(column)
                    .getType(), column);
            columns[column] = col;
        }
        return col;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    class ChainedValueVector implements ValueVector
    {
        private final ResolvedType type;
        private final int column;

        // Cached values optimized for sequential access of rows
        private ValueVector currentVector;
        private int currentVectorUpperBound = -1;
        private int currentVectorLowerBound = -1;

        ChainedValueVector(ResolvedType type, int column)
        {
            this.type = type;
            this.column = column;
        }

        @Override
        public ResolvedType type()
        {
            return type;
        }

        @Override
        public int size()
        {
            return rowCount;
        }

        @Override
        public boolean isNull(int row)
        {
            setCurrentVector(row);
            return currentVector.isNull(row - currentVectorLowerBound);
        }

        @Override
        public boolean getBoolean(int row)
        {
            setCurrentVector(row);
            return currentVector.getBoolean(row - currentVectorLowerBound);
        }

        @Override
        public int getInt(int row)
        {
            setCurrentVector(row);
            return currentVector.getInt(row - currentVectorLowerBound);
        }

        @Override
        public long getLong(int row)
        {
            setCurrentVector(row);
            return currentVector.getLong(row - currentVectorLowerBound);
        }

        @Override
        public float getFloat(int row)
        {
            setCurrentVector(row);
            return currentVector.getFloat(row - currentVectorLowerBound);
        }

        @Override
        public double getDouble(int row)
        {
            setCurrentVector(row);
            return currentVector.getDouble(row - currentVectorLowerBound);
        }

        @Override
        public Decimal getDecimal(int row)
        {
            setCurrentVector(row);
            return currentVector.getDecimal(row - currentVectorLowerBound);
        }

        @Override
        public UTF8String getString(int row)
        {
            setCurrentVector(row);
            return currentVector.getString(row - currentVectorLowerBound);
        }

        @Override
        public EpochDateTime getDateTime(int row)
        {
            setCurrentVector(row);
            return currentVector.getDateTime(row - currentVectorLowerBound);
        }

        @Override
        public EpochDateTimeOffset getDateTimeOffset(int row)
        {
            setCurrentVector(row);
            return currentVector.getDateTimeOffset(row - currentVectorLowerBound);
        }

        @Override
        public ObjectVector getObject(int row)
        {
            setCurrentVector(row);
            return currentVector.getObject(row - currentVectorLowerBound);
        }

        @Override
        public ValueVector getArray(int row)
        {
            setCurrentVector(row);
            return currentVector.getArray(row - currentVectorLowerBound);
        }

        @Override
        public TupleVector getTable(int row)
        {
            setCurrentVector(row);
            return currentVector.getTable(row - currentVectorLowerBound);
        }

        @Override
        public Object getAny(int row)
        {
            setCurrentVector(row);
            return currentVector.getAny(row - currentVectorLowerBound);
        }

        private void setCurrentVector(int row)
        {
            // Vector already set
            if (row >= currentVectorLowerBound
                    && row <= currentVectorUpperBound)
            {
                return;
            }

            int index = getIndex(row);
            currentVectorLowerBound = index == 0 ? 0
                    : ordinals[index - 1];
            currentVectorUpperBound = ordinals[index] - 1;

            TupleVector tupleVector = vectors.get(index);

            // Current vector's schema doesn't have this column, return null
            if (column >= tupleVector.getSchema()
                    .getSize())
            {
                currentVector = ValueVector.literalNull(type, rowCount);
            }
            else
            {
                currentVector = tupleVector.getColumn(column);
            }
        }

        private int getIndex(int row)
        {
            /*
             * @formatter:off
             * 
             * 3 vectors with their row indices
             * [0-4] (0-4)
             * [0-4] (5-9)
             * [0-4] (10-14)
             * 
             * Ordinals array: [5,10,15]
             * 
             * Total index:            0,  1,  2,  3,  4   5,  6,  7,  8,  9   10, 11, 12, 13, 14
             * Index in vector:      [[0,  1,  2,  3,  4],[0,  1,  2,  3,  4],[ 0,  1,  2,  3,  4]]
             * Binary search result:  -1, -1, -1, -1, -1   0, -2, -2, -2, -2    1, -3, -3, -3, -3
             * @formatt:on
             */
            
            // Ordinals are row count + 1 in the source vectors
            int index = Arrays.binarySearch(ordinals, row);
            // Result is either the index or (-(insertion point) - 1)
            index++;
            if (index < 0)
            {
                index = -index;
            }
            return index;
        }
    }

    /**
     * Created a chained tuple vector. This returns a single tuple vector that wraps a list of source vectors. This requires that the provided vectors shares a common subset of their schema.
     */
    public static TupleVector chain(List<TupleVector> vectors)
    {
        int size = vectors.size();
        
        if (size == 0)
        {
            return TupleVector.EMPTY;
        }
        else if (size == 1)
        {
            return vectors.get(0);
        }
        
        // Validate schema. All schemas must share the same columns or have less/more columns
        // ie.
        // Schema1
        // col1 int
        // col2 boolean
        //
        // Schema2
        // col1 int
        //
        // Schema3
        // col1 int
        // col2 boolean
        // col3 string
        //
        // The vector with the largest schema will be the resulting schema
        
        Schema schema = vectors.get(0).getSchema();
        int[] ordinals = new int[size];
        int rowCount = vectors.get(0).getRowCount();
        ordinals[0] = rowCount;
        
        for (int i=1;i<size;i++)
        {
            rowCount += vectors.get(i).getRowCount();
            ordinals[i] = rowCount;
            
            Schema current = vectors.get(i).getSchema();
            // Optimize for chaining a lot of the same type of vectors, then we avoid a lot
            // of column comparisons
            if (schema == current)
            {
                continue;
            }
            
            int colCount = Math.min(schema.getSize(), current.getSize());
            for (int j=0;j<colCount;j++)
            {
                if (!schema.getColumns().get(j).equals(current.getColumns().get(j)))
                {
                    throw new IllegalArgumentException("Schema of chained tuple vectors must share a common sub set of columns.");
                }
            }
            
            // Switch to the largest schema
            if (current.getSize() > schema.getSize())
            {
                schema = current;
            }
        }
        return new ChainedTupleVector(schema, vectors, rowCount, ordinals);
    }
}
