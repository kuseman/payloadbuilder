package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;

/** Utils used for writing Vectors */
public class OutputWriterUtils
{
    /** Write this object vector to provided output writer */
    public static void write(ObjectVector vector, OutputWriter writer, IExecutionContext context)
    {
        Schema schema = vector.getSchema();
        int size = schema.getColumns()
                .size();

        int row = vector.getRow();
        // Store the columns to avoid accessing those for each row
        // since they could have computations inside
        ValueVector[] values = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            values[i] = vector.getValue(i);
        }

        writer.startObject();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            String columnName = column.getName();
            writer.writeFieldName(columnName);
            ValueVector valueVector = values[i];
            write(valueVector, row, writer, context);
        }
        writer.endObject();
    }

    /** Write this tuple vector to provided output writer */
    public static void write(TupleVector vector, OutputWriter writer, IExecutionContext context, boolean root)
    {
        Schema schema = vector.getSchema();
        int size = schema.getColumns()
                .size();
        int rowCount = vector.getRowCount();

        if (!root)
        {
            writer.startArray();
        }

        // Store the columns to avoid accessing those for each row
        // since they could have computations inside
        ValueVector[] columns = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            if (column instanceof CoreColumn
                    && ((CoreColumn) column).isInternal())
            {
                continue;
            }
            columns[i] = vector.getColumn(i);
        }

        for (int j = 0; j < rowCount; j++)
        {
            if (root)
            {
                writer.startRow();
            }
            writer.startObject();
            for (int i = 0; i < size; i++)
            {
                Column column = schema.getColumns()
                        .get(i);

                if (columns[i] == null)
                {
                    continue;
                }

                String columnName = column.getName();
                if (column instanceof CoreColumn)
                {
                    columnName = ((CoreColumn) column).getOutputName();
                }
                writer.writeFieldName(columnName);
                ValueVector valueVector = columns[i];
                write(valueVector, j, writer, context);
            }
            writer.endObject();
            if (root)
            {
                writer.endRow();
            }
        }
        if (!root)
        {
            writer.endArray();
        }
    }

    /** Write this value vector to provided output writer */
    public static void write(ValueVector vector, OutputWriter writer, IExecutionContext context)
    {
        int size = vector.size();
        writer.startArray();
        for (int i = 0; i < size; i++)
        {
            write(vector, i, writer, context);
        }
        writer.endArray();
    }

    /** Write provided row to provided output writer */
    public static void write(ValueVector vector, int row, OutputWriter writer, IExecutionContext context)
    {
        if (vector.isNull(row))
        {
            writer.writeValue(null);
            return;
        }
        // CSOFF
        switch (vector.type()
                .getType())
        // CSON
        {
            case Boolean:
                writer.writeBool(vector.getBoolean(row));
                break;
            case Double:
                writer.writeDouble(vector.getDouble(row));
                break;
            case Float:
                writer.writeFloat(vector.getFloat(row));
                break;
            case Int:
                writer.writeInt(vector.getInt(row));
                break;
            case Long:
                writer.writeLong(vector.getLong(row));
                break;
            case Decimal:
                // TODO: add support for decimals in outputwriter
                writer.writeValue(vector.getDecimal(row)
                        .asBigDecimal());
                break;
            case Any:
                // Runtime check of value

                Object value = vector.getAny(row);
                value = VectorUtils.convert(value);

                // Reflectively check if the value is a vector of some sort
                if (value instanceof ValueVector)
                {
                    write((ValueVector) value, writer, context);
                }
                else if (value instanceof ObjectVector)
                {
                    write((ObjectVector) value, writer, context);
                }
                else if (value instanceof TupleVector)
                {
                    write((TupleVector) value, writer, context, false);
                }
                else if (value instanceof Integer)
                {
                    writer.writeInt(((Integer) value).intValue());
                }
                else if (value instanceof Long)
                {
                    writer.writeLong(((Long) value).longValue());
                }
                else if (value instanceof Float)
                {
                    writer.writeFloat(((Float) value).floatValue());
                }
                else if (value instanceof Double)
                {
                    writer.writeDouble(((Double) value).doubleValue());
                }
                else if (value instanceof String
                        || value instanceof UTF8String)
                {
                    writer.writeValue(String.valueOf(value));
                }
                else if (value instanceof Decimal)
                {
                    writer.writeValue(((Decimal) value).asBigDecimal());
                }
                else if (value instanceof EpochDateTime)
                {
                    writer.writeValue(((EpochDateTime) value).toString());
                }
                else if (value instanceof EpochDateTimeOffset)
                {
                    writer.writeValue(((EpochDateTimeOffset) value).toString());
                }
                else
                {
                    writer.writeValue(value);
                }
                break;
            case String:
                // TODO: add support for UTF-8 bytes in outputwriter
                writer.writeValue(vector.getString(row)
                        .toString());
                break;
            case DateTime:
                writer.writeValue(vector.getDateTime(row)
                        .toString());
                break;
            case DateTimeOffset:
                writer.writeValue(vector.getDateTimeOffset(row)
                        .toString());
                break;
            case Object:
                ObjectVector object = vector.getObject(row);
                write(object, writer, context);
                break;
            case Table:
                TupleVector tupleVector = vector.getTable(row);
                write(tupleVector, writer, context, false);
                break;
            case Array:
                ValueVector valueVector = vector.getArray(row);
                write(valueVector, writer, context);
                break;
            // NOTE!! No default case here
        }
    }
}
