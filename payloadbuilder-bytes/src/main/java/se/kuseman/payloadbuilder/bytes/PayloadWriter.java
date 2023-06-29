package se.kuseman.payloadbuilder.bytes;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Payload writer that compresses and writes payload according to a Schema.
 */
public class PayloadWriter
{
    private PayloadWriter()
    {
    }

    /** Writes vector to bytes */
    public static byte[] write(ValueVector vector)
    {
        requireNonNull(vector, "vector");

        BytesWriter writer = new BytesWriter();

        writer.putByte(PayloadReader.P);
        writer.putByte(PayloadReader.L);
        writer.putByte(PayloadReader.B);
        writer.putVarInt(PayloadReader.VERSION);

        WriteCache cache = new WriteCache();
        writeVector(writer, cache, vector, 0, vector.size(), true);

        // Put a last byte used as checksum
        writer.putByte(writer.getByte(PayloadReader.CHECKSUM_BYTE));

        return writer.toBytes();
    }

    static void writeVector(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to)
    {
        writeVector(writer, cache, vector, from, to, false);
    }

    private static void writeVector(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, boolean root)
    {
        /*
         * @formatter:off
         * Vector
         * - type bytes (See Utils#writeResolvedType) (only written on root vectors)
         * - length (varInt)
         * - nullLength (varInt)
         * - nullByte0
         * - nullByte1
         * - nullByteX
         * - vector implementation version (byte)
         * - data
         * @formatter:on
         */

        Type type = vector.type()
                .getType();

        // We only need the full resolved type on root
        if (root)
        {
            Utils.writeResolvedType(writer, vector.type());
        }

        int length = to - from;

        // Length
        writer.putVarInt(length);

        int nullCount = NullBufferWriter.writeNullBuffer(writer, vector, from, to);

        // Literal null, no need to write any more data
        if (nullCount == length)
        {
            return;
        }

        // TODO: if vector is Any then try to convert the vector to a known type before proceeding

        VectorWriter vectorWriter = VectorWriterFactory.getWriter(type);

        // Version
        writer.putByte(vectorWriter.getVersion());

        vectorWriter.write(writer, cache, vector, from, to, nullCount);
    }
}
