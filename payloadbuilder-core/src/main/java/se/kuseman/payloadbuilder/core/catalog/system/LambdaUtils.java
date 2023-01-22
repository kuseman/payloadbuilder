package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;

/** Common functionality to process lambda expressions/functions */
class LambdaUtils
{
    /** Value vector wrapper that acts as a single row tuple vector. Used in each iteration for lambdas */
    static class RowTupleVector implements TupleVector
    {
        private TupleVector wrapped;
        private int row;
        private int rowCount = 1;

        RowTupleVector(TupleVector vector)
        {
            this.wrapped = vector;
        }

        void setRow(int row)
        {
            this.row = row;
        }

        void setRowCount(int rowCount)
        {
            this.rowCount = rowCount;
        }

        @Override
        public int getRowCount()
        {
            return rowCount;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            final ValueVector vector = wrapped.getColumn(column);
            return new ValueVectorAdapter(vector)
            {
                @Override
                public int size()
                {
                    return rowCount;
                }

                @Override
                protected int getRow(int row)
                {
                    return RowTupleVector.this.row;
                }
            };
        }

        @Override
        public Schema getSchema()
        {
            return wrapped.getSchema();
        }
    }
}
