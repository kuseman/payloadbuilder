package se.kuseman.payloadbuilder.api.execution;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** Definition of a TupleVector. Consists of a {@link Schema} and a list of {@link ValueVector}'s */
public interface TupleVector
{
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

    /** Copy one row from this instance into a new tuple vector. */
    default TupleVector copy(int row)
    {
        Schema schema = getSchema();
        int size = schema.getSize();
        List<ValueVector> vectors = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ValueVector vector = getColumn(i);
            Type type = vector.type()
                    .getType();
            if (vector.isNull(row))
            {
                vectors.add(ValueVector.literalNull(vector.type(), 1));
                continue;
            }

            vectors.add(switch (type)
            {
                case Any -> ValueVector.literalAny(1, vector.getAny(row));
                case Array -> ValueVector.literalArray(vector.getArray(row), 1);
                case Boolean -> ValueVector.literalBoolean(vector.getBoolean(row), 1);
                case DateTime -> vector.getDateTime(row);
                case DateTimeOffset -> vector.getDateTimeOffset(row);
                case Decimal -> vector.getDecimal(row);
                case String -> vector.getString(row);
                case Double -> ValueVector.literalDouble(vector.getDouble(row), 1);
                case Float -> ValueVector.literalFloat(vector.getFloat(row), 1);
                case Int -> ValueVector.literalInt(vector.getInt(row), 1);
                case Long -> ValueVector.literalLong(vector.getLong(row), 1);
                case Object -> ValueVector.literalObject(vector.getObject(row), 1);
                case Table -> ValueVector.literalTable(vector.getTable(row), 1);
            });
        }
        return TupleVector.of(schema, vectors);
    }

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
            throw new IllegalArgumentException("Empty vector has no columns");
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
            throw new IllegalArgumentException("Constant has no columns");
        }
    };

    /** Create a zero rows {@link TupleVector} with provided schema. */
    static TupleVector of(final Schema schema)
    {
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
                return 0;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return ValueVector.empty(schema.getColumns()
                        .get(column)
                        .getType());
            }
        };
    }

    /** Construct a {@link TupleVector} from provided columns and schema */
    static TupleVector of(Schema schema, ValueVector... columns)
    {
        return of(schema, List.of(columns));
    }

    /** Construct a {@link TupleVector} from provided columns and schema */
    static TupleVector of(final Schema schema, final List<? extends ValueVector> columns)
    {
        if (columns.isEmpty())
        {
            return TupleVector.of(schema);
        }

        final int rowCount = columns.isEmpty() ? 0
                : columns.get(0)
                        .size();
        // Validate the vectors against the schema
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
            throw new IllegalArgumentException("Schema column count doesn't match vector count. Schema: " + schema.getSize() + ", vectors: " + columnSize);
        }

        for (int i = 0; i < columnSize; i++)
        {
            ResolvedType schemaType = schema.getColumns()
                    .get(i)
                    .getType();
            ResolvedType vectorType = columns.get(i)
                    .type();

            validate(schema.getColumns()
                    .get(i)
                    .getName(), schemaType, vectorType);
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
                if (vv.isNull(i))
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
                        case Array:
                            value = vv.getArray(i)
                                    .toCsv(indent);
                            break;
                        case Table:
                            value = vv.getTable(i)
                                    .toCsv(indent + 1);
                            break;
                        case Object:
                            value = vv.getObject(i);
                            break;
                        default:
                            value = vv.getAny(i);
                            if (value instanceof ValueVector)
                            {
                                value = ((ValueVector) value).toCsv(indent);
                            }
                            break;
                    }
                    sb.append(String.valueOf(value));
                }

                int padLength = Math.max(maxColLenghts[j] - (sb.length() - currentLength), 0);
                sb.append(StringUtils.repeat(' ', padLength));
            }
            sb.append(System.lineSeparator())
                    .append(pad);
        }
        return sb.toString();
    }

    private static void validate(String columnPath, ResolvedType schemaType, ResolvedType vectorType)
    {
        // Don't validate any types, they can differ, reflection will be used runtime
        if (schemaType.getType() == Type.Any)
        {
            return;
        }

        if (schemaType.getType() != vectorType.getType())
        {
            throw new IllegalArgumentException("Schema type: " + schemaType.getType() + " for column: " + columnPath + " doesn't match value vectors type " + vectorType);
        }

        // If this is not a complex type we're done
        if (schemaType.getSchema() == null)
        {
            return;
        }

        Schema vectorSchema = vectorType.getSchema();

        // Skip validation of empty vector schemas
        if (Schema.EMPTY.equals(vectorSchema))
        {
            return;
        }

        Schema schemaSchema = schemaType.getSchema();
        int size = schemaSchema.getSize();

        if (size != vectorSchema.getSize())
        {
            throw new IllegalArgumentException("Schema size: " + size + " for column: " + columnPath + " doesn't match value vectors size: " + vectorSchema.getSize());
        }

        for (int i = 0; i < size; i++)
        {
            Column schemaColumn = schemaSchema.getColumns()
                    .get(i);
            Column vectorColumn = vectorSchema.getColumns()
                    .get(i);
            validate(columnPath + "/" + schemaColumn, schemaColumn.getType(), vectorColumn.getType());
        }
    }
}
