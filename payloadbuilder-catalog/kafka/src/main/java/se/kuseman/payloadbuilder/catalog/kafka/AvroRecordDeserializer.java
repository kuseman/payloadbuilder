package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.catalog.kafka.SchemaRegistryClient.RegisteredSchema;

/** Deserializes Avro-encoded values using Schema Registry for schema resolution */
class AvroRecordDeserializer implements IRecordDeserializer
{
    private static final byte MAGIC_BYTE = 0x0;

    private final IntFunction<RegisteredSchema> schemaLookup;
    private final Map<Integer, org.apache.avro.Schema> schemaCache = new ConcurrentHashMap<>();

    AvroRecordDeserializer(String schemaRegistryUrl)
    {
        this(createClient(schemaRegistryUrl)::get);
    }

    /** Create a deserializer that resolves schema ids with the given lookup, for example a shared {@link SchemaRegistryClient}. */
    AvroRecordDeserializer(IntFunction<RegisteredSchema> schemaLookup)
    {
        this.schemaLookup = requireNonNull(schemaLookup, "schemaLookup");
    }

    private static SchemaRegistryClient createClient(String schemaRegistryUrl)
    {
        if (schemaRegistryUrl == null
                || schemaRegistryUrl.isBlank())
        {
            throw new IllegalArgumentException("schema_registry_url catalog property is required for Avro format");
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
        return UTF8String.from(new String(keyBytes, StandardCharsets.UTF_8));
    }

    @Override
    public Object deserializeValue(byte[] valueBytes)
    {
        if (valueBytes == null)
        {
            return null;
        }
        return decodeAvro(valueBytes);
    }

    private Object decodeAvro(byte[] bytes)
    {
        if (bytes.length < 5)
        {
            throw new RuntimeException("Avro payload too short (expected at least 5 bytes for wire format header)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte magic = buffer.get();
        if (magic != MAGIC_BYTE)
        {
            throw new RuntimeException("Invalid Avro wire format magic byte: " + magic);
        }

        int schemaId = buffer.getInt();
        org.apache.avro.Schema schema = schemaCache.computeIfAbsent(schemaId, this::resolveSchema);

        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);

        try
        {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get()
                    .binaryDecoder(payload, null);
            GenericRecord record = reader.read(null, decoder);
            return genericRecordToMap(record);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to decode Avro payload with schema id " + schemaId, e);
        }
    }

    private org.apache.avro.Schema resolveSchema(int schemaId)
    {
        RegisteredSchema registered = schemaLookup.apply(schemaId);
        if (!SchemaRegistryClient.AVRO.equals(registered.schemaType()))
        {
            throw new RuntimeException("Schema id " + schemaId + " is " + registered.schemaType() + ", not Avro");
        }
        try
        {
            return new org.apache.avro.Schema.Parser().parse(registered.schema());
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException("Failed to parse Avro schema id " + schemaId, e);
        }
    }

    /** Convert a GenericRecord into a Map for Type.Any consumption */
    static Object convertAvroValue(Object value)
    {
        if (value == null)
        {
            return null;
        }
        if (value instanceof GenericRecord record)
        {
            return genericRecordToMap(record);
        }
        if (value instanceof Utf8 utf8)
        {
            return UTF8String.from(utf8.toString());
        }
        if (value instanceof CharSequence cs)
        {
            return UTF8String.from(cs.toString());
        }
        if (value instanceof List<?> list)
        {
            List<Object> result = new ArrayList<>(list.size());
            for (Object item : list)
            {
                result.add(convertAvroValue(item));
            }
            return result;
        }
        if (value instanceof Map<?, ?> map)
        {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet())
            {
                result.put(String.valueOf(entry.getKey()), convertAvroValue(entry.getValue()));
            }
            return result;
        }
        if (value instanceof ByteBuffer bb)
        {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return bytes;
        }
        // Primitives (Integer, Long, Float, Double, Boolean) pass through
        return value;
    }

    static Map<String, Object> genericRecordToMap(GenericRecord record)
    {
        Map<String, Object> map = new HashMap<>();
        for (org.apache.avro.Schema.Field field : record.getSchema()
                .getFields())
        {
            map.put(field.name(), convertAvroValue(record.get(field.name())));
        }
        return map;
    }
}
