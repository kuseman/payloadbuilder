package se.kuseman.payloadbuilder.bytes;

import static java.util.Collections.singletonMap;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Utils when woring with writing/reading vectors */
class Utils
{
    private Utils()
    {
    }

    /**
     * Internal (not part of {@link MetaData}'s public constants) key used to stamp the physical slot a reconciled column was found at in the payload. Only ever present on schemas produced by
     * {@link VectorFactory#recreateSchema(Schema, Schema, boolean)} when column-id based reconciliation ran. Absent (null) means "no remap, use the logical index as-is".
     */
    static final String PHYSICAL_SLOT_KEY = "__pb_physical_slot";

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

    // Table/Object variants that additionally carry a column id per column. Self-describing: a reader that
    // doesn't know about these byte values fails immediately with "Unknown type byte" instead of silently
    // misparsing, so no separate payload/type version needs to be threaded through this class at all.
    private static final byte TABLE_WITH_IDS_BYTE = 11;
    private static final byte OBJECT_WITH_IDS_BYTE = 12;

    private static final Map<Byte, Type> BYTE_TO_TYPE_MAP = TYPE_TO_BYTE_MAP.entrySet()
            .stream()
            .collect(Collectors.toMap(kv -> kv.getValue(), kv -> kv.getKey()));
    static
    {
        BYTE_TO_TYPE_MAP.put(TABLE_WITH_IDS_BYTE, Type.Table);
        BYTE_TO_TYPE_MAP.put(OBJECT_WITH_IDS_BYTE, Type.Object);
    }

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
     * Compute a fast, order-sensitive hash (FNV-1a) over the given byte range. Used to fingerprint a payload's type-tree bytes for {@link SchemaReconciliationCache} - a fingerprint collision would
     * only ever cause a false cache hit within the same expected {@link Schema} instance and {@code expand} flag, and even then only matters if paired with a genuinely different type tree, which is
     * astronomically unlikely for a 64 bit hash.
     */
    static long hashRange(ByteBuffer buffer, int from, int to)
    {
        long hash = 0xcbf29ce484222325L;
        for (int i = from; i < to; i++)
        {
            hash ^= buffer.get(i) & 0xFF;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    /** Returns true if every column in provided schema has a column id assigned. False if schema is empty. */
    static boolean allColumnsHaveIds(Schema schema)
    {
        int size = schema.getSize();
        if (size == 0)
        {
            return false;
        }
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < size; i++)
        {
            if (columns.get(i)
                    .getMetaData()
                    .getColumnId() < 0)
            {
                return false;
            }
        }
        return true;
    }

    /** Returns true if any column in provided schema has a column id assigned. */
    private static boolean anyColumnHasId(Schema schema)
    {
        List<Column> columns = schema.getColumns();
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            if (columns.get(i)
                    .getMetaData()
                    .getColumnId() >= 0)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine if provided schema should be written using column ids. Requires every column to have an id, throws if the schema mixes columns with and without an id since that leaves the
     * reconciliation logic no safe way to know which identity model to trust.
     */
    private static boolean columnIdWriteMode(Schema schema)
    {
        boolean any = anyColumnHasId(schema);
        boolean all = allColumnsHaveIds(schema);
        if (any
                && !all)
        {
            throw new IllegalArgumentException("Schema mixes columns with and without a column id, this is not supported: " + schema);
        }

        if (all)
        {
            Set<Integer> seenIds = new HashSet<>();
            List<Column> columns = schema.getColumns();
            int size = columns.size();
            for (int i = 0; i < size; i++)
            {
                int id = columns.get(i)
                        .getMetaData()
                        .getColumnId();
                if (!seenIds.add(id))
                {
                    throw new IllegalArgumentException("Duplicate column id " + id + " in schema: " + schema);
                }
            }
        }

        return all;
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
     * If the table/object's columns are all tagged with a column id, byte 11/12 (instead of 9/10) is written
     * and each column is prefixed with its id (varInt) before its type.
     *
     * </pre>
     */
    static void writeResolvedType(BytesWriter writer, ResolvedType resolvedType)
    {
        Type type = resolvedType.getType();

        if (type == Type.Table
                || type == Type.Object)
        {
            Schema schema = resolvedType.getSchema();
            int size = schema.getSize();
            boolean hasIds = columnIdWriteMode(schema);

            writer.putByte(type == Type.Table ? (hasIds ? TABLE_WITH_IDS_BYTE
                    : TYPE_TO_BYTE_MAP.get(Type.Table))
                    : (hasIds ? OBJECT_WITH_IDS_BYTE
                            : TYPE_TO_BYTE_MAP.get(Type.Object)));

            writer.putVarInt(size);

            for (int i = 0; i < size; i++)
            {
                Column column = schema.getColumns()
                        .get(i);
                if (hasIds)
                {
                    writer.putVarInt(column.getMetaData()
                            .getColumnId());
                }
                writeResolvedType(writer, column.getType());
            }
            return;
        }

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
    }

    /** Reads a resolved type from buffer at it's current position. Returns the buffer at position after type data */
    static int readResolvedType(ByteBuffer buffer, int position, Reference<ResolvedType> ref)
    {
        byte rawType = buffer.get(position++);
        Type type = getType(rawType);

        if (type == Type.Array)
        {
            Reference<ResolvedType> arrayType = new Reference<>();
            position = readResolvedType(buffer, position, arrayType);
            ref.set(ResolvedType.array(arrayType.getValue()));
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            boolean hasIds = rawType == TABLE_WITH_IDS_BYTE
                    || rawType == OBJECT_WITH_IDS_BYTE;

            int size = readVarInt(buffer, position);
            position += Utils.sizeOfVarInt(size);

            List<Column> columns = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                int columnId = -1;
                if (hasIds)
                {
                    columnId = readVarInt(buffer, position);
                    position += sizeOfVarInt(columnId);
                }

                Reference<ResolvedType> columnType = new Reference<>();
                position = readResolvedType(buffer, position, columnType);

                String name = columnType.getValue()
                        .getType()
                        .toString()
                        .toLowerCase() + "_" + i;
                MetaData metaData = hasIds ? new MetaData(singletonMap(MetaData.COLUMN_ID, columnId))
                        : MetaData.EMPTY;

                columns.add(Column.of(name, columnType.getValue(), metaData));
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
     * unless the schema is column-id tagged in which case a mismatch is flagged whenever the payload's column at a given position doesn't carry the same id as expected's column at that position -
     * this forces the slower {@link VectorFactory#recreateSchema(Schema, Schema, boolean)} path which is the one that knows how to resolve columns by id instead of position.
     */
    static int validateResolvedType(ByteBuffer buffer, int position, ResolvedType expected, BooleanReference typeMismatch)
    {
        // Read next type from buffer
        byte rawType = buffer.get(position++);
        Type type = getType(rawType);
        if (expected != null
                && type != expected.getType())
        {
            // Flag that we have a mismatch to get used later on
            typeMismatch.set(true);
        }

        if (type == Type.Array)
        {
            position = validateResolvedType(buffer, position, expected != null ? expected.getSubType()
                    : null, typeMismatch);
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            Schema expectedSchema = expected != null ? expected.getSchema()
                    : null;
            int expectedSize = expectedSchema != null ? expectedSchema.getSize()
                    : 0;

            boolean payloadHasIds = rawType == TABLE_WITH_IDS_BYTE
                    || rawType == OBJECT_WITH_IDS_BYTE;

            int size = readVarInt(buffer, position);
            position += sizeOfVarInt(size);

            boolean idMode = payloadHasIds
                    && expectedSchema != null
                    && allColumnsHaveIds(expectedSchema);

            for (int i = 0; i < size; i++)
            {
                int columnId = -1;
                if (payloadHasIds)
                {
                    columnId = readVarInt(buffer, position);
                    position += sizeOfVarInt(columnId);
                }

                Column expectedSubColumn = i < expectedSize ? expectedSchema.getColumns()
                        .get(i)
                        : null;

                if (idMode
                        && expectedSubColumn != null
                        && expectedSubColumn.getMetaData()
                                .getColumnId() != columnId)
                {
                    // Column at this physical position doesn't carry the id we expect there -> schema drift,
                    // force the slower reconciliation path that resolves columns by id
                    typeMismatch.set(true);
                }

                ResolvedType expectedColumnType = expectedSubColumn != null ? expectedSubColumn.getType()
                        : null;
                position = validateResolvedType(buffer, position, expectedColumnType, typeMismatch);
            }
        }
        return position;
    }

    /** Validates and expands provided type against the buffers type */
    static int expandType(ByteBuffer buffer, int position, ResolvedType expected, Reference<ResolvedType> ref, BooleanReference typeMismatch)
    {
        byte rawType = buffer.get(position++);
        Type type = getType(rawType);
        if (expected != null
                && type != expected.getType())
        {
            typeMismatch.set(true);
        }

        if (type == Type.Array)
        {
            Reference<ResolvedType> arrayType = new Reference<>();
            position = expandType(buffer, position, expected != null ? expected.getSubType()
                    : null, arrayType, typeMismatch);
            ref.set(ResolvedType.array(arrayType.getValue()));
        }
        else if (type == Type.Table
                || type == Type.Object)
        {
            Schema expectedSchema = expected != null ? expected.getSchema()
                    : null;
            int expectedSize = expectedSchema != null ? expectedSchema.getSize()
                    : 0;

            boolean payloadHasIds = rawType == TABLE_WITH_IDS_BYTE
                    || rawType == OBJECT_WITH_IDS_BYTE;

            int size = readVarInt(buffer, position);
            position += sizeOfVarInt(size);

            boolean idMode = payloadHasIds
                    && expectedSchema != null
                    && allColumnsHaveIds(expectedSchema);

            List<Column> columns = new ArrayList<>(Math.max(size, expectedSize));

            for (int i = 0; i < size; i++)
            {
                int columnId = -1;
                if (payloadHasIds)
                {
                    columnId = readVarInt(buffer, position);
                    position += sizeOfVarInt(columnId);
                }

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

                if (idMode
                        && col.getMetaData()
                                .getColumnId() != columnId)
                {
                    typeMismatch.set(true);
                }

                ResolvedType expectedColumnType = col.getType();

                Reference<ResolvedType> result = new Reference<>();
                position = expandType(buffer, position, expectedColumnType, result, typeMismatch);
                columns.add(new Column(col.getName(), result.getValue(), col.getMetaData()));
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
