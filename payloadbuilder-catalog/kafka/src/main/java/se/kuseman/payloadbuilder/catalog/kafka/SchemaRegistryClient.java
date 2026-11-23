package se.kuseman.payloadbuilder.catalog.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.catalog.HttpClientUtils;

/**
 * Looks up schemas by id in a Confluent Schema Registry. Must not reference any Avro class: it's used by {@link AutoRecordDeserializer} to route values when the optional avro library isn't on the
 * classpath.
 */
class SchemaRegistryClient
{
    static final String AVRO = "AVRO";
    static final String JSON = "JSON";
    static final String PROTOBUF = "PROTOBUF";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> urls;
    private final Map<Integer, RegisteredSchema> cache = new ConcurrentHashMap<>();
    /**
     * Failed lookups. A client lives for one datasource execution (it's created with the deserializer in KafkaDatasource.execute), so a failure is remembered for the rest of it: with on_error =
     * 'skip' or 'null' every value would otherwise make new HTTP calls, each up to the connect or read timeout.
     */
    private final Map<Integer, RuntimeException> failures = new ConcurrentHashMap<>();

    SchemaRegistryClient(String urls)
    {
        this.urls = parseUrls(urls);
    }

    List<String> getUrls()
    {
        return urls;
    }

    /** Return the schema with the given id. Throws if no registry URL could serve it; a failed id throws the same exception again without a new lookup. */
    RegisteredSchema get(int schemaId)
    {
        RuntimeException failure = failures.get(schemaId);
        if (failure != null)
        {
            throw failure;
        }
        try
        {
            return cache.computeIfAbsent(schemaId, this::fetch);
        }
        catch (RuntimeException e)
        {
            failures.putIfAbsent(schemaId, e);
            throw e;
        }
    }

    private RegisteredSchema fetch(int schemaId)
    {
        Exception lastException = null;
        for (String baseUrl : urls)
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
                    return fromResponse(schemaId, responseMap);
                });
            }
            catch (Exception e)
            {
                lastException = e;
            }
        }
        throw new RuntimeException("Failed to fetch schema id " + schemaId + " from all Schema Registry URLs: " + urls, lastException);
    }

    /** Map a registry {@code /schemas/ids/<id>} response. A missing {@code schemaType} means Avro. */
    static RegisteredSchema fromResponse(int schemaId, Map<String, Object> response)
    {
        Object type = response.get("schemaType");
        String schemaType = type == null ? AVRO
                : String.valueOf(type)
                        .toUpperCase();
        Object schema = response.get("schema");
        return new RegisteredSchema(schemaId, schemaType, schema == null ? null
                : String.valueOf(schema));
    }

    static List<String> parseUrls(String urls)
    {
        if (urls == null
                || urls.isBlank())
        {
            throw new IllegalArgumentException("schema_registry_url catalog property is required for schema-registry values");
        }
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
        return List.copyOf(result);
    }

    /**
     * A schema from the registry.
     *
     * @param id Schema id
     * @param schemaType {@code AVRO}, {@code JSON}, {@code PROTOBUF} or another type the registry reports, upper case
     * @param schema The schema text
     */
    record RegisteredSchema(int id, String schemaType, String schema)
    {
    }
}
