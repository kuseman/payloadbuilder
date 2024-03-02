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
    static final byte LITERAL_ENCODING = 0;
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
     * Migration strategy for reusing non used column slots.
     * -----------------------------------------------------
     *
     * When doing development and re-modelling columns it's often the case that we end up with unused columns in the "middle".
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
        requireNonNull(bytes);
        requireNonNull(schema);

        ReadContext context = new ReadContext(schema, expandSchema);

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
