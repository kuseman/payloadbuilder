package se.kuseman.payloadbuilder.catalog.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.catalog.HttpClientUtils;

/** Deserializes Avro-encoded values using Schema Registry for schema resolution */
class AvroRecordDeserializer implements IRecordDeserializer
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte MAGIC_BYTE = 0x0;

    private final List<String> schemaRegistryUrls;
    private final Map<Integer, org.apache.avro.Schema> schemaCache = new ConcurrentHashMap<>();

    AvroRecordDeserializer(String schemaRegistryUrl)
    {
        if (schemaRegistryUrl == null
                || schemaRegistryUrl.isBlank())
        {
            throw new IllegalArgumentException("schema_registry_url catalog property is required for Avro format");
        }
        this.schemaRegistryUrls = parseUrls(schemaRegistryUrl);
    }

    private static List<String> parseUrls(String urls)
    {
        List<String> result = new ArrayList<>();
        for (String url : urls.split(","))
        {
            String trimmed = url.trim();
            if (!trimmed.isEmpty())
            {
                // Normalize: strip trailing slash
                result.add(trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1)
                        : trimmed);
            }
        }
        if (result.isEmpty())
        {
            throw new IllegalArgumentException("schema_registry_url must contain at least one URL");
        }
        return result;
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
        org.apache.avro.Schema schema = schemaCache.computeIfAbsent(schemaId, this::fetchSchema);

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

    private org.apache.avro.Schema fetchSchema(int schemaId)
    {
        Exception lastException = null;
        for (String baseUrl : schemaRegistryUrls)
        {
            String url = baseUrl + "/schemas/ids/" + schemaId;
            try
            {
                HttpGet get = new HttpGet(url);
                return HttpClientUtils.execute(null, get, null, null, null, null, null, null, response ->
                {
                    if (response.getCode() != HttpStatus.SC_OK)
                    {
                        String body = IOUtils.toString(response.getEntity()
                                .getContent(), StandardCharsets.UTF_8);
                        throw new RuntimeException("Failed to fetch schema id " + schemaId + " from " + url + ": " + response.getCode() + " " + body);
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> responseMap = MAPPER.readValue(response.getEntity()
                            .getContent(), Map.class);
                    String schemaJson = (String) responseMap.get("schema");
                    return new org.apache.avro.Schema.Parser().parse(schemaJson);
                });
            }
            catch (Exception e)
            {
                lastException = e;
            }
        }
        throw new RuntimeException("Failed to fetch Avro schema id " + schemaId + " from all Schema Registry URLs: " + schemaRegistryUrls, lastException);
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
