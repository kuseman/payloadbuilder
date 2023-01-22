package se.kuseman.payloadbuilder.api.catalog;

import java.util.List;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** Definition of a TupleVector. This is the result from executing a {@link IPhysicalPlan}. */
public interface TupleVector
{
    static final TupleVector EMPTY = new TupleVector()
    {
        @Override
        public Schema getSchema()
        {
            return Schema.EMPTY;
        }

        @Override
        public int getRowCount()
        {
            return 0;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return null;
        }
    };

    /** Return a constant tuple vector with no schema and 1 row */
    static final TupleVector CONSTANT = new TupleVector()
    {
        @Override
        public Schema getSchema()
        {
            return Schema.EMPTY;
        }

        @Override
        public int getRowCount()
        {
            return 1;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            throw new IllegalArgumentException("Constant scan has no columns");
        }
    };

    int getRowCount();

    /**
     * Return vector for provided column. NOTE! Returned value might not be thread safe.
     */
    ValueVector getColumn(int column);

    /**
     * Return the actual schema from the vector. If this tuple vector resides from a schema less query then this is the actual columns resolved runtime else this schema should match the compile time
     * schema that the data source had
     */
    Schema getSchema();

    /** Write this tuple vector to provided output writer */
    default void write(OutputWriter writer, IExecutionContext context, boolean root)
    {
        Schema schema = getSchema();
        int size = schema.getColumns()
                .size();
        int rowCount = getRowCount();

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
            if (column.isInternal())
            {
                continue;
            }
            columns[i] = getColumn(i);
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

                if (column.isInternal())
                {
                    continue;
                }

                String columnName = column.getOutputName();
                writer.writeFieldName(columnName);
                ValueVector vector = columns[i];
                vector.write(j, writer, context);
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

    /** Generate csv (tab separated) string of this vector */
    default String toCsv()
    {
        return toCsv(0);
    }

    /** Generate csv (tab separated) string of this vector */
    default String toCsv(int indent)
    {
        String pad = StringUtils.repeat(' ', indent);

        StringBuilder sb = new StringBuilder(pad);
        Schema s = getSchema();

        int[] maxColLenghts = new int[s.getSize()];
        for (int i = 0; i < s.getSize(); i++)
        {
            Column c = s.getColumns()
                    .get(i);
            int maxLength = c.getName()
                    .length();
            int colIndex = s.getColumns()
                    .indexOf(c);
            ValueVector col = getColumn(colIndex);
            int size = getRowCount();
            for (int j = 0; j < size; j++)
            {
                if (col.isNull(j))
                {
                    maxLength = Math.max(maxLength, "null".length());
                }
                else
                {
                    // TODO: valuevector/tuplevector
                    maxLength = Math.max(maxLength, String.valueOf(col.valueAsObject(j))
                            .length());
                }
            }
            maxColLenghts[i] = maxLength + 1;
        }

        s.getColumns()
                .stream()
                .forEach(c ->
                {
                    int index = s.getColumns()
                            .indexOf(c);
                    int maxLength = maxColLenghts[index];
                    sb.append(c.getName());
                    int padLength = maxLength - c.getName()
                            .length();
                    sb.append(StringUtils.repeat(' ', padLength));
                });
        if (sb.length() > 0)
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(System.lineSeparator())
                .append(pad);
        int rowCount = getRowCount();
        int colCount = s.getSize();
        for (int i = 0; i < rowCount; i++)
        {
            for (int j = 0; j < colCount; j++)
            {
                ValueVector vv = getColumn(j);

                int currentLength = sb.length();
                if (vv.isNullable()
                        && vv.isNull(i))
                {
                    sb.append("null");
                }
                else
                {
                    Object value;
                    switch (vv.type()
                            .getType())
                    {
                        case Boolean:
                            value = vv.getBoolean(i);
                            break;
                        case Double:
                            value = vv.getDouble(i);
                            break;
                        case Float:
                            value = vv.getFloat(i);
                            break;
                        case Int:
                            value = vv.getInt(i);
                            break;
                        case Long:
                            value = vv.getLong(i);
                            break;
                        case String:
                            value = vv.getString(i)
                                    .toString();
                            break;
                        case DateTime:
                            value = vv.getDateTime(i)
                                    .toString();
                            break;
                        case ValueVector:
                            value = ((ValueVector) vv.getValue(i)).toCsv(indent);
                            break;
                        case TupleVector:
                            value = ((TupleVector) vv.getValue(i)).toCsv(indent + 1);
                            break;
                        default:
                            value = vv.getValue(i);
                            if (value instanceof ValueVector)
                            {
                                value = ((ValueVector) value).toCsv(indent);
                            }
                            break;
                    }
                    sb.append(String.valueOf(value));
                }

                int padLength = maxColLenghts[j] - (sb.length() - currentLength);
                sb.append(StringUtils.repeat(' ', padLength));
            }
            sb.append(System.lineSeparator())
                    .append(pad);
        }
        return sb.toString();
    }

    /** Construct a {@link TupleVector} from provided columns and schema */
    static TupleVector of(final Schema schema, final List<ValueVector> columns)
    {
        final int rowCount = columns.get(0)
                .size();
        final int columnSize = columns.size();
        for (ValueVector vv : columns)
        {
            if (vv.size() != rowCount)
            {
                throw new IllegalArgumentException("All vectors must equal in size");
            }
        }
        if (schema.getColumns()
                .size() != columnSize)
        {
            throw new IllegalArgumentException("Schema column count doesn't match vector count.");
        }

        for (int i = 0; i < schema.getColumns()
                .size(); i++)
        {
            if (!schema.getColumns()
                    .get(i)
                    .getType()
                    .equals(columns.get(i)
                            .type()))
            {
                throw new IllegalArgumentException("Schema type for column: " + schema.getColumns()
                        .get(i)
                                                   + " doesn't match value vectors type "
                                                   + columns.get(i)
                                                           .type());
            }
        }

        return new TupleVector()
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
                return columns.get(column);
            }

            @Override
            public String toString()
            {
                return "TupleVector";
            }
        };
    }
}
