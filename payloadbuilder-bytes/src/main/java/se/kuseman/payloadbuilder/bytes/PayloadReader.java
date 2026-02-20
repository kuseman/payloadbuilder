package se.kuseman.payloadbuilder.bytes;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Reader that transforms written payloads to {@link ValueVector}'s */
public class PayloadReader
{
    static final int CHECKSUM_BYTE = 7;

    private PayloadReader()
    {
    }

    static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;
    static final int VERSION = 2;
    static final byte P = 'P';
    static final byte L = 'L';
    static final byte B = 'B';
    static final byte REGULAR_LITERAL_ENCODING = 0;
    static final byte REGULAR_ENCODING = 1;

    /** Checks if provided payload is valid payload */
    public static boolean isSupportedPayload(byte[] bytes)
    {
        if (bytes == null)
        {
            return false;
        }

        // An empty Vector occupies at least 8 bytes so any smaller than this cannot be valid
        if (bytes.length < 8)
        {
            return false;
        }

        byte checksumByte = bytes[CHECKSUM_BYTE];

        // Validate headers and checksum
        if (bytes[0] == P
                && bytes[1] == L
                && bytes[2] == B
                && bytes[bytes.length - 1] == checksumByte)
        {
            return true;
        }

        return false;
    }

    /**
     * Return value vector from provided payload.
     */
    public static ValueVector read(byte[] bytes)
    {
        requireNonNull(bytes);
        return readInternal(bytes, new ReadContext());
    }

    /**
     * Reads a tuple vector from provided payload. NOTE! Assumes the vector written are of type Table and of size 1
     *
     * <pre>
     * Schema evolution
     * ================
     *
     * There are two ways to evolve a schema over time without an older or newer reader misreading a payload:
     * column ids (recommended for new schemas) and the legacy positional strategy kept below for schemas that
     * predate column ids or that choose not to use them.
     *
     * Column id based evolution (recommended)
     * ----------------------------------------
     *
     * Assign every column of a schema level a stable {@link se.kuseman.payloadbuilder.api.catalog.Column.MetaData#COLUMN_ID}.
     * Once assigned, an id must never be reused for a different column - everything else about the schema is
     * free to change. Columns can be added, removed or reordered anywhere, not just at the end, and a column's
     * name/type can still be corrected using the exact same implicit-cast rules described in the legacy strategy
     * below, since reconciliation now happens by id instead of position - where a column physically sits no
     * longer matters.
     *
     * Ie.
     *
     * Table
     *   - id=1 column1 INT
     *   - id=2 column2 BOOL
     *   - id=3 column3 STRING
     *
     * A new column can be inserted anywhere and an old one removed, without disturbing anyone else's id:
     *
     * Table
     *   - id=1 column1      INT
     *   - id=4 newFancyArray ARRAY[INT]   (new column, inserted in the middle - id=2/column2 removed)
     *   - id=3 column3      STRING
     *
     * A reader still on the old schema (expecting id=2 "column2") simply gets a null column for it. "newFancyArray"
     * (id=4, unknown to that reader) is ignored, unless expandSchema is set to true in which case it's appended
     * with a generated name.
     *
     * Requirements, enforced when writing: every column of a schema level must either have an id or none of them
     * may (mixing throws), and ids must be unique within a schema level (duplicates throw). Id based
     * reconciliation only engages when *both* the payload and the reader's expected schema carry ids - if either
     * side is missing them, reconciliation silently falls back to the positional strategy below, so payloads
     * written before column ids were adopted keep working unchanged.
     *
     * Positional migration strategy (legacy - append/reuse only)
     * ------------------------------------------------------------
     *
     * Without column ids a column's identity is purely its position in the schema, so during development and
     * re-modelling of columns it's often the case that we end up with unused columns in the "middle" that need
     * to be reused in place, rather than removed, since removing or inserting a column anywhere but the end
     * would silently misread an older/newer schema's data.
     * Ie.
     *
     * Table
     *   - column1 INT
     *   - column2 BOOL
     *   - column3 STRING
     *
     * And later on column2 is wrong and we add a new column with correct data:
     *
     * Table
     *   - column1 INT
     *   - column2 BOOL  (now obsolete and unused)
     *   - column3 STRING
     *   - column4 INT   (corrected from column2)
     *
     * Now we have a correct model and everything is good, except that we have an unused column slot that is waste.
     * In these cases we can make sure that the column in question is unused and then we are safe to change the type
     * and payloadbuilder-bytes will transform the internal schema into the payloads type while still keeping the input schema
     * as is. This to be consistent with resolved queries that expects a specific schema.
     *
     * This enables us to change a columns datatype/name as long as it's null or it's value can be implicitly cast to new type
     *
     * Ie.
     *
     * Original
     *
     * Table
     *   - column1 INT
     *   - column2 BOOL     (Unused)
     *   - column3 STRING
     *   - column4 INT
     *
     * A new version is written that looks like this:
     *
     * Table
     *   - column1       INT
     *   - newFancyArray ARRAY[INT]  (Now used in new version of clients)
     *   - column3       STRING
     *   - column4       INT
     *
     * Old client versions uses the old schema will read payloads that results in:
     *
     * Table
     *   - column1 INT
     *   - column2 BOOL     (Unused)
     *   - column3 STRING
     *   - column4 INT
     *
     *   Internal types of the payload (this will be the actual types used)
     *   - column1 INT
     *   - column2 ARRAY[INT]     (Still unused but now has the type from the payload but with the old name, this is
     *                            safe as long as we either don't use the column at all or the implicit cast will work
     *                            ie. we changed from int to float then the query will read int and we will cast the float to an int.)
     *   - column3 STRING
     *   - column4 INT
     * </pre>
     *
     * @param bytes The payload
     * @param schema Schema to use as a verification against the payload. If the schema's types differs from the payloads then a new schema is created that matches the payloads.
     * @param expandSchema Set to true if columns that is not present in schema should be added. This is useful if one doesn't know know fully how the data looks and want to inspect.
     */
    public static TupleVector readTupleVector(byte[] bytes, Schema schema, boolean expandSchema)
    {
        return readTupleVector(bytes, schema, expandSchema, null);
    }

    /**
     * Same as {@link #readTupleVector(byte[], Schema, boolean)} but additionally letting repeated reads of payloads whose schema has drifted from {@code schema} skip re-deriving the reconciled schema
     * every time, as long as the same {@link SchemaReconciliationCache} instance is reused across calls. See that class' javadoc for when this actually helps and what it requires of the caller.
     *
     * @param reconciliationCache Cache to use for schema reconciliation, or null to reconcile fresh on every call as {@link #readTupleVector(byte[], Schema, boolean)} does.
     */
    public static TupleVector readTupleVector(byte[] bytes, Schema schema, boolean expandSchema, SchemaReconciliationCache reconciliationCache)
    {
        requireNonNull(bytes);
        requireNonNull(schema);

        ReadContext context = new ReadContext(schema, expandSchema, reconciliationCache);

        ValueVector vector = readInternal(bytes, context);
        return vector.getTable(0);
    }

    private static ValueVector readInternal(byte[] bytes, ReadContext context)
    {
        if (bytes.length < 8
                || (!(bytes[0] == P
                        && bytes[1] == L
                        && bytes[2] == B)
                        || bytes[bytes.length - 1] != bytes[CHECKSUM_BYTE]))
        {
            throw new IllegalArgumentException("Illegal payload. Expected marker bytes does not exists");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes)
                .order(BYTE_ORDER);
        int position = 3;
        int version = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(version);
        if (version != VERSION)
        {
            throw new IllegalArgumentException("Unsupported version of payload: " + version);
        }

        return VectorFactory.getVector(buffer, position, context, null);
    }
}
