package se.kuseman.payloadbuilder.bytes;

import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Utils when woring with writing/reading vectors */
class Utils
{
    private Utils()
    {
    }

    //@formatter:off
    private static final Map<Type, Byte> TYPE_TO_BYTE_MAP = new EnumMap<>(ofEntries(
            entry(Type.Boolean, (byte) 0),
            entry(Type.Int, (byte) 1),
            entry(Type.Long, (byte) 2),
            entry(Type.Float, (byte) 3),
            entry(Type.Double, (byte) 4),
            entry(Type.String, (byte) 5),
            entry(Type.DateTime, (byte) 6),
            entry(Type.Decimal, (byte) 7),
            entry(Type.Array, (byte) 8),
            entry(Type.Table, (byte) 9),
            entry(Type.Object, (byte) 10)
            ));
    //@formatter:on

    private static final Map<Byte, Type> BYTE_TO_TYPE_MAP = TYPE_TO_BYTE_MAP.entrySet()
            .stream()
            .collect(Collectors.toMap(kv -> kv.getValue(), kv -> kv.getKey()));

    /**
     * Read variable int at provided position
     */
    static int readVarInt(ByteBuffer buffer, int position)
    {
        byte b = buffer.get(position++);

        if (b == (byte) 0x80)
        {
            throw new RuntimeException("Attempting to read null value as int");
        }

        int value = b & 0x7F;
        while ((b & 0x80) != 0)
        {
            b = buffer.get(position++);
            value <<= 7;
            value |= (b & 0x7F);
        }

        return value;
    }

    /** Return bytes needed for provided var int value */
    static short sizeOfVarInt(int value)
    {
        if (value < 0)
        {
            throw new IllegalArgumentException("negative value");
        }
        short cnt = 0;
        //@formatter:off
        //CSOFF
        if(value > 0x0FFFFFFF) { cnt++; }
        if(value > 0x1FFFFF)   { cnt++; }
        if(value > 0x3FFF)     { cnt++; }
        if(value > 0x7F)       { cnt++; }
        //CSON
        //@formatter:on

        cnt++;
        return cnt;
    }

    /**
     * Writes provided resolved type. Writes recursive type bytes if type is complex
     * 
     * <pre>
     * Ie.
     * 
     * Table
     *   int
     *   float
     *   array
     *     int
     *   table
     *     string
     *     float
     *     int
     * 
     * Will write:
     * 
     * int (1)
     * float (3)
     * array (8)
     *   int (1)
     * table (9)
     *   columnLength: (varInt = 3)
     *   string (5)
     *   float (3)
     *   int (1)
     *   
     * Resulting bytes: [1,3,8,1,1,9,3,5,3,1]
     * 
     * </pre>
     */
    static void writeResolvedType(BytesWriter writer, ResolvedType resolvedType)
    {
        Type type = resolvedType.getType();
        Byte b = TYPE_TO_BYTE_MAP.get(type);
        if (b == null)
        {
            throw new IllegalArgumentException("Type " + type + " in unsupported for writing");
        }

        writer.putByte(b);
        if (type == Type.Array)
        {
            writeResolvedType(writer, resolvedType.getSubType());
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            Schema schema = resolvedType.getSchema();
            int size = schema.getSize();
            writer.putVarInt(size);
            for (int i = 0; i < size; i++)
            {
                writeResolvedType(writer, schema.getColumns()
                        .get(i)
                        .getType());
            }
        }
    }

    /** Reads a resolved type from buffer at it's current position. Returns the buffer at position after type data */
    static int readResolvedType(ByteBuffer buffer, int position, Reference<ResolvedType> ref)
    {
        Type type = getType(buffer.get(position++));

        if (type == Type.Array)
        {
            Reference<ResolvedType> arrayType = new Reference<>();
            position = readResolvedType(buffer, position, arrayType);
            ref.set(ResolvedType.array(arrayType.getValue()));
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            int size = readVarInt(buffer, position);
            position += Utils.sizeOfVarInt(size);
            List<Column> columns = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                Reference<ResolvedType> columnType = new Reference<>();
                position = readResolvedType(buffer, position, columnType);
                columns.add(Column.of(columnType.getValue()
                        .getType()
                        .toString()
                        .toLowerCase() + "_" + i, columnType.getValue()));
            }
            Schema schema = new Schema(columns);

            ref.set(type == Type.Table ? ResolvedType.table(schema)
                    : ResolvedType.object(schema));
        }
        else
        {
            ref.set(ResolvedType.of(type));
        }
        return position;
    }

    /**
     * Validates the provided resolved type against the payload's. Throwing if not equal. NOTE! On Table/Object types the schema does not need to be equal but those ordinals that exists must equal
     */
    static int validateResolvedType(ByteBuffer buffer, int position, ResolvedType expected)
    {
        // Read next type from buffer
        Type type = getType(buffer.get(position++));
        if (expected != null
                && type != expected.getType())
        {
            throw new IllegalArgumentException("Payload type was " + type + " but provided type was: " + expected);
        }
        else if (type == Type.Array)
        {
            position = validateResolvedType(buffer, position, expected != null ? expected.getSubType()
                    : null);
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            Schema expectedSchema = expected != null ? expected.getSchema()
                    : null;
            int expectedSize = expectedSchema != null ? expectedSchema.getSize()
                    : 0;
            int size = readVarInt(buffer, position);
            position += sizeOfVarInt(size);
            for (int i = 0; i < size; i++)
            {
                ResolvedType expectedColumnType = i < expectedSize ? expectedSchema.getColumns()
                        .get(i)
                        .getType()
                        : null;
                position = validateResolvedType(buffer, position, expectedColumnType);
            }
        }
        return position;
    }

    /** Validates and expands provided type against the buffers type */
    static int expandType(ByteBuffer buffer, int position, ResolvedType expected, Reference<ResolvedType> ref)
    {
        Type type = getType(buffer.get(position++));
        if (expected != null
                && type != expected.getType())
        {
            throw new IllegalArgumentException("Payload type was " + type + " but provided type was: " + expected);
        }
        else if (type == Type.Array)
        {
            Reference<ResolvedType> arrayType = new Reference<>();
            position = expandType(buffer, position, expected != null ? expected.getSubType()
                    : null, arrayType);
            ref.set(ResolvedType.array(arrayType.getValue()));
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            Schema expectedSchema = expected != null ? expected.getSchema()
                    : null;
            int expectedSize = expectedSchema != null ? expectedSchema.getSize()
                    : 0;
            int size = readVarInt(buffer, position);
            position += sizeOfVarInt(size);
            List<Column> columns = new ArrayList<>(Math.max(size, expectedSize));

            for (int i = 0; i < size; i++)
            {
                // Column that does not exists in expected, read type from payload and add a generated column name
                if (i >= expectedSize)
                {
                    Reference<ResolvedType> columnType = new Reference<>();
                    position = readResolvedType(buffer, position, columnType);
                    columns.add(Column.of(columnType.getValue()
                            .getType()
                            .toString()
                            .toLowerCase() + "_" + i, columnType.getValue()));
                    continue;
                }

                Column col = expectedSchema.getColumns()
                        .get(i);
                ResolvedType expectedColumnType = col.getType();

                Reference<ResolvedType> result = new Reference<>();
                position = expandType(buffer, position, expectedColumnType, result);
                columns.add(Column.of(col.getName(), result.getValue()));
            }

            // More columns in expected schema than payload, add those, will yield null when accessed
            if (expectedSize > size)
            {
                for (int i = size; i < expectedSize; i++)
                {
                    columns.add(expectedSchema.getColumns()
                            .get(i));
                }
            }

            Schema schema = new Schema(columns);
            ref.set(type == Type.Table ? ResolvedType.table(schema)
                    : ResolvedType.object(schema));
        }
        else
        {
            ref.set(ResolvedType.of(type));
        }
        return position;
    }

    /** Get type from provided byte */
    private static Type getType(byte b)
    {
        Type t = BYTE_TO_TYPE_MAP.get(b);
        if (t == null)
        {
            throw new IllegalArgumentException("Unknown type byte: " + b);
        }
        return t;
    }
}
