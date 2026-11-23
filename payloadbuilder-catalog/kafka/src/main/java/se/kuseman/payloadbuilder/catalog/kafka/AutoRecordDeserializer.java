package se.kuseman.payloadbuilder.catalog.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.catalog.kafka.SchemaRegistryClient.RegisteredSchema;

/**
 * Detects each value's format: schema-registry wire format (routed on the registry's schema type), JSON, or else UTF-8 text. See documentation/kafka-auto-format.md.
 */
class AutoRecordDeserializer implements IRecordDeserializer
{
    /** Confluent wire format: magic byte 0x00 and a 4-byte big-endian schema id */
    private static final int WIRE_HEADER_LENGTH = 5;
    private static final byte MAGIC_BYTE = 0x0;
    /** Marks a value that isn't JSON. A JSON {@code null} literal parses to {@code null}, so null can't be used. */
    private static final Object NOT_JSON = new Object();
    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);

    /** Schema lookup, null when the catalog has no schema registry URL */
    private final IntFunction<RegisteredSchema> schemaLookup;
    private final Supplier<IRecordDeserializer> avroFactory;

    private IRecordDeserializer avro;
    private boolean avroMissing;

    AutoRecordDeserializer(String schemaRegistryUrl)
    {
        this(createClient(schemaRegistryUrl));
    }

    private AutoRecordDeserializer(SchemaRegistryClient client)
    {
        this(client == null ? null
                : client::get,
                client == null ? null
                        : () -> new AvroRecordDeserializer(client::get));
    }

    /**
     * Test constructor.
     *
     * @param schemaLookup Schema lookup, or null for no registry
     * @param avroFactory Creates the Avro delegate on the first Avro value. Not called when schemaLookup is null.
     */
    AutoRecordDeserializer(IntFunction<RegisteredSchema> schemaLookup, Supplier<IRecordDeserializer> avroFactory)
    {
        this.schemaLookup = schemaLookup;
        this.avroFactory = avroFactory;
    }

    private static SchemaRegistryClient createClient(String schemaRegistryUrl)
    {
        if (schemaRegistryUrl == null
                || schemaRegistryUrl.isBlank())
        {
            return null;
        }
        return new SchemaRegistryClient(schemaRegistryUrl);
    }

    @Override
    public Object deserializeKey(byte[] keyBytes)
    {
        if (keyBytes == null)
        {
            return null;
        }
        return RawRecordDeserializer.toText(keyBytes);
    }

    @Override
    public Object deserializeValue(byte[] valueBytes)
    {
        if (valueBytes == null)
        {
            return null;
        }

        if (isWireFormat(valueBytes))
        {
            int schemaId = readSchemaId(valueBytes);
            if (schemaLookup != null)
            {
                return deserializeRegistered(valueBytes, schemaId);
            }
            // No registry: a JSON Schema value is JSON after the header. Anything else reads as text.
            Object json = tryJson(valueBytes, WIRE_HEADER_LENGTH);
            if (json != NOT_JSON)
            {
                return json;
            }
            return RawRecordDeserializer.toText(valueBytes);
        }

        Object json = tryJson(valueBytes, 0);
        if (json != NOT_JSON)
        {
            return json;
        }
        return RawRecordDeserializer.toText(valueBytes);
    }

    private Object deserializeRegistered(byte[] bytes, int schemaId)
    {
        RegisteredSchema schema = schemaLookup.apply(schemaId);
        String schemaType = schema.schemaType();
        if (SchemaRegistryClient.AVRO.equals(schemaType))
        {
            return deserializeAvro(bytes, schemaId);
        }
        else if (SchemaRegistryClient.JSON.equals(schemaType))
        {
            try
            {
                return JsonRecordDeserializer.parseStrict(bytes, WIRE_HEADER_LENGTH, bytes.length - WIRE_HEADER_LENGTH);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to deserialize JSON Schema value (schema id " + schemaId + ")", e);
            }
        }
        else if (SchemaRegistryClient.PROTOBUF.equals(schemaType))
        {
            throw new RuntimeException("Value is a Protobuf schema-registry value (schema id " + schemaId + "); Protobuf isn't supported");
        }
        throw new RuntimeException("Value has unsupported schema type '" + schemaType + "' (schema id " + schemaId + ")");
    }

    private Object deserializeAvro(byte[] bytes, int schemaId)
    {
        if (avroMissing)
        {
            throw new RuntimeException(avroMissingMessage(schemaId));
        }
        try
        {
            if (avro == null)
            {
                avro = avroFactory.get();
            }
            return avro.deserializeValue(bytes);
        }
        catch (LinkageError e)
        {
            // NoClassDefFoundError and friends: the optional avro dependency isn't on the classpath. Don't retry the load.
            avroMissing = true;
            throw new RuntimeException(avroMissingMessage(schemaId), e);
        }
    }

    private static String avroMissingMessage(int schemaId)
    {
        return "Value is in Avro schema-registry format (schema id " + schemaId + ") but the avro library isn't on the classpath";
    }

    @Override
    public void close()
    {
        if (avro != null)
        {
            avro.close();
        }
    }

    private static boolean isWireFormat(byte[] bytes)
    {
        return bytes.length >= WIRE_HEADER_LENGTH
                && bytes[0] == MAGIC_BYTE;
    }

    private static int readSchemaId(byte[] bytes)
    {
        return ((bytes[1] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 8) | (bytes[4] & 0xFF);
    }

    /**
     * Parse the bytes from {@code offset} as JSON when the first significant byte could start a JSON value and the value spans the rest of the input. Returns {@link #NOT_JSON} otherwise, so text
     * costs a byte check rather than a failed parse.
     */
    private static Object tryJson(byte[] bytes, int offset)
    {
        int index = skipBomAndWhitespace(bytes, offset);
        if (index >= bytes.length
                || !mayBeJson(bytes, index))
        {
            return NOT_JSON;
        }
        try
        {
            return JsonRecordDeserializer.parseStrict(bytes, index, bytes.length - index);
        }
        catch (IOException e)
        {
            return NOT_JSON;
        }
    }

    private static int skipBomAndWhitespace(byte[] bytes, int offset)
    {
        int index = offset;
        if (bytes.length - index >= 3
                && (bytes[index] & 0xFF) == 0xEF
                && (bytes[index + 1] & 0xFF) == 0xBB
                && (bytes[index + 2] & 0xFF) == 0xBF)
        {
            index += 3;
        }
        while (index < bytes.length
                && isWhitespace(bytes[index]))
        {
            index++;
        }
        return index;
    }

    /**
     * Cheap pre-check so that text costs a few byte comparisons rather than a thrown parse exception. Objects, arrays and strings are only recognised by their first byte. A literal must be the whole
     * value, and a number may only contain number characters, so text such as {@code true story} or {@code 2026-11-22 INFO ...} is rejected here.
     */
    private static boolean mayBeJson(byte[] bytes, int index)
    {
        byte b = bytes[index];
        if (b == '{'
                || b == '['
                || b == '"')
        {
            return true;
        }
        else if (b == 't')
        {
            return isWholeLiteral(bytes, index, TRUE);
        }
        else if (b == 'f')
        {
            return isWholeLiteral(bytes, index, FALSE);
        }
        else if (b == 'n')
        {
            return isWholeLiteral(bytes, index, NULL);
        }
        else if (b == '-'
                || (b >= '0'
                        && b <= '9'))
        {
            return isNumberCharacters(bytes, index);
        }
        return false;
    }

    /** The literal followed only by whitespace */
    private static boolean isWholeLiteral(byte[] bytes, int index, byte[] literal)
    {
        if (bytes.length - index < literal.length)
        {
            return false;
        }
        for (int i = 0; i < literal.length; i++)
        {
            if (bytes[index + i] != literal[i])
            {
                return false;
            }
        }
        return isWhitespaceFrom(bytes, index + literal.length);
    }

    /** Only characters a JSON number can contain, then only whitespace. Stops at the first other byte. */
    private static boolean isNumberCharacters(byte[] bytes, int index)
    {
        int i = index;
        while (i < bytes.length)
        {
            byte b = bytes[i];
            if ((b >= '0'
                    && b <= '9')
                    || b == '-'
                    || b == '+'
                    || b == '.'
                    || b == 'e'
                    || b == 'E')
            {
                i++;
                continue;
            }
            break;
        }
        return isWhitespaceFrom(bytes, i);
    }

    private static boolean isWhitespaceFrom(byte[] bytes, int index)
    {
        for (int i = index; i < bytes.length; i++)
        {
            if (!isWhitespace(bytes[i]))
            {
                return false;
            }
        }
        return true;
    }

    private static boolean isWhitespace(byte b)
    {
        return b == ' '
                || b == '\t'
                || b == '\r'
                || b == '\n';
    }
}
