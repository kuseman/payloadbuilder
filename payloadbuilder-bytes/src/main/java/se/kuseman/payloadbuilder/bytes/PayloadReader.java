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
     * @param bytes The payload
     * @param schema Schema to use as a verification against the payload. If the schema's types differs from the payloads an exception is thrown.
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
