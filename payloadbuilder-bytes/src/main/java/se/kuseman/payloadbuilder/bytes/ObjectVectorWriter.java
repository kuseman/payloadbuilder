package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer that writes Vectors of type {@link Column.Type#Object} */
class ObjectVectorWriter implements VectorWriter
{
    static ObjectVectorWriter INSTANCE = new ObjectVectorWriter();

    @Override
    public byte getVersion()
    {
        return se.kuseman.payloadbuilder.bytes.ObjectVector.VERSION;
    }

    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        /*
         * @formatter:off
         * Layout
         * 
         * - encoding (1 byte)
         * - columnCount (varInt)
         * - tupleVector
         * 
         * @formatter:on
         */

        writer.putByte(PayloadReader.REGULAR_ENCODING);

        final Schema schema = vector.type()
                .getSchema();

        int size = vector.type()
                .getSchema()
                .getSize();

        writer.putVarInt(size);

        // We write an object vector as a single tuple vector to avoid to much meta data for all individual vectors
        final int rowCount = to - from;
        TupleVector tupleVector = new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                ResolvedType type = schema.getColumns()
                        .get(column)
                        .getType();
                return new ObjectValueVector(vector, rowCount, column, type);
            }
        };

        TableVectorWriter.writeTupleVector(writer, cache, tupleVector, size);
    }

    /** Wrapper class that wraps a value in an ObjectVector */
    static class ObjectValueVector implements ValueVector
    {
        /** Vector with ObjectVectors */
        private final ValueVector wrapped;
        private final int rowCount;
        private final int column;
        private final ResolvedType type;

        ObjectValueVector(ValueVector wrapped, int rowCount, int column, ResolvedType type)
        {
            this.wrapped = wrapped;
            this.rowCount = rowCount;
            this.column = column;
            this.type = type;
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
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.isNull(object.getRow());
        }

        @Override
        public Object getAny(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getAny(object.getRow());
        }

        @Override
        public boolean getBoolean(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getBoolean(object.getRow());
        }

        @Override
        public int getInt(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getInt(object.getRow());
        }

        @Override
        public long getLong(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getLong(object.getRow());
        }

        @Override
        public float getFloat(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getFloat(object.getRow());
        }

        @Override
        public double getDouble(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getDouble(object.getRow());
        }

        @Override
        public Decimal getDecimal(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getDecimal(object.getRow());
        }

        @Override
        public UTF8String getString(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getString(object.getRow());
        }

        @Override
        public EpochDateTime getDateTime(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getDateTime(object.getRow());
        }

        @Override
        public EpochDateTimeOffset getDateTimeOffset(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getDateTimeOffset(object.getRow());
        }

        @Override
        public ObjectVector getObject(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getObject(object.getRow());
        }

        @Override
        public ValueVector getArray(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getArray(object.getRow());
        }

        @Override
        public TupleVector getTable(int row)
        {
            ObjectVector object = wrapped.getObject(row);
            ValueVector value = object.getValue(column);
            return value.getTable(object.getRow());
        }
    }
}
