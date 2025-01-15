package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Utils used to build meta about an elastic search instance */
class ElasticsearchMetaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchMetaUtils.class);
    private static final int MAX_INDICES_TO_FETCH = 10;
    private static final QualifiedName CACHE_NAME = QualifiedName.of("ElasticSearch", "Meta");
    private static final String CACHE_MAPPINGS_TTL = "cache.mappings.ttl";
    /** Default cache time for mappings */
    private static final int MAPPINGS_CACHE_TTL = 60;

    // CSOFF
    //@formatter:off
    private record DataStreamsResponse(List<DataStream> data_streams){}
    private record DataStream(String template){}

    private record IndexTemplatesResponse(List<IndexTemplateTop> index_templates){}
    private record IndexTemplateTop(IndexTemplate index_template, String name){}
    private record IndexTemplate(IndexTemplateTemplate template){}
    private record IndexTemplateTemplate(Map<String, Object> mappings){}
    //@formatter:on
    // CSON

    /** Returns properties for current sessions endpoint/index */
    @SuppressWarnings("unchecked")
    static ElasticsearchMeta getMeta(IQuerySession session, String catalogAlias, String endpoint, String index)
    {
        if (isBlank(endpoint)
                || isBlank(index))
        {
            throw new IllegalArgumentException("Missing endpoint/index in catalog properties with alias: " + catalogAlias);
        }
        int ttl = session.getCatalogProperty(catalogAlias, CACHE_MAPPINGS_TTL, ValueVector.literalInt(MAPPINGS_CACHE_TTL, 1))
                .getInt(0);
        QualifiedName key = QualifiedName.of(endpoint, index);
        return session.getGenericCache()
                .computIfAbsent(CACHE_NAME, key, Duration.ofMinutes(ttl), () ->
                {
                    // Fetch version
                    Map<String, Object> map = get(Map.class, session, catalogAlias, endpoint, "");
                    String versionString = (String) ((Map<String, Object>) map.get("version")).get("number");
                    ElasticsearchMeta.Version version = ElasticsearchMeta.Version.fromString(versionString);

                    Map<String, MappedType> result = new HashMap<>();

                    map = get(Map.class, session, catalogAlias, endpoint, index + "/_alias");

                    List<List<String>> partitions;

                    // No aliases -> simply use the input index
                    if (map.isEmpty())
                    {
                        partitions = singletonList(singletonList(index));
                    }
                    else
                    {
                        List<String> allIndices = new ArrayList<>(map.keySet());
                        // Batch up indices and retrieve mappings in partitions
                        // to avoid memory propblems since some aliases can contain serveral hundreds
                        // of concrete indices
                        partitions = ListUtils.partition(allIndices, MAX_INDICES_TO_FETCH);
                    }

                    for (List<String> parition : partitions)
                    {
                        String indices = parition.stream()
                                .collect(joining(","));

                        map = get(Map.class, session, catalogAlias, endpoint, indices + "/_mappings");

                        // Traverse all indices matching mappings
                        // if this is a multi indices query like wildcard (*) or
                        // comma separated then we will have multiple indices to fetch properties
                        // for and should be merged
                        for (Entry<String, Object> indexEntry : map.entrySet())
                        {
                            Object obj = indexEntry.getValue();

                            Map<String, Object> currentIndexMappings = (Map<String, Object>) obj;
                            Map<String, Object> mappings = (Map<String, Object>) currentIndexMappings.get("mappings");
                            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

                            // Single type ES version, return dummy table_name
                            if (properties != null
                                    && !properties.containsKey("properties"))
                            {
                                mergeProperties(result, ESCatalog.SINGLE_TYPE_TABLE_NAME, mappings, properties, indexEntry.getKey());
                            }
                            else
                            {
                                Map<String, Object> allMeta = new HashMap<>();
                                Map<QualifiedName, MappedProperty> allProperties = new HashMap<>();

                                // Old es version with types
                                for (Entry<String, Object> e : mappings.entrySet())
                                {
                                    Map<String, Object> currentMappings = (Map<String, Object>) e.getValue();
                                    properties = (Map<String, Object>) currentMappings.getOrDefault("properties", emptyMap());

                                    allMeta.putAll(currentMappings);

                                    mergeProperties(result, e.getKey(), currentMappings, properties, indexEntry.getKey());
                                }

                                // Store all mappings under fake _doc table to be able to query multi type
                                for (MappedType mappedType : result.values())
                                {
                                    allProperties.putAll(mappedType.properties);
                                }

                                // This is a version of ES that still has type mappings but
                                // don't support more than one type so clear out all types
                                // and use _doc for everything
                                if (!version.getStrategy()
                                        .supportsTypes())
                                {
                                    result.clear();
                                }

                                MappedType docType = new MappedType(allMeta, allProperties);
                                result.put(ESCatalog.SINGLE_TYPE_TABLE_NAME, docType);
                            }
                        }
                    }

                    if (version.getStrategy()
                            .supportsDataStreams())
                    {
                        // Fetch data stream mappings
                        appendDataStreamMappings(session, catalogAlias, endpoint, index, result);
                    }

                    return new ElasticsearchMeta(version, result);
                });
    }

    private static void appendDataStreamMappings(IQuerySession session, String catalogAlias, String endpoint, String index, Map<String, MappedType> result)
    {
        try
        {
            DataStreamsResponse datastreams = get(DataStreamsResponse.class, session, catalogAlias, endpoint, "_data_stream/" + index);
            Set<String> seenIndexTemplates = new HashSet<>();

            for (DataStream dsi : datastreams.data_streams)
            {
                if (seenIndexTemplates.add(dsi.template))
                {
                    IndexTemplatesResponse indexTemplatesResponse = get(IndexTemplatesResponse.class, session, catalogAlias, endpoint, "_index_template/" + dsi.template);
                    for (IndexTemplateTop itt : indexTemplatesResponse.index_templates)
                    {
                        if (itt.index_template != null
                                && itt.index_template.template != null
                                && itt.index_template.template.mappings != null)
                        {
                            Map<String, Object> mappings = itt.index_template.template.mappings;
                            @SuppressWarnings("unchecked")
                            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

                            mergeProperties(result, ESCatalog.SINGLE_TYPE_TABLE_NAME, mappings, properties, itt.name);
                        }
                    }

                }
            }
        }
        catch (Exception e)
        {
            // Skip logging error about alias
            if (e.getMessage()
                    .toLowerCase()
                    .contains("specify the corresponding concrete indices instead"))
            {
                return;
            }
            // Swallow this since all ES versions doesn't have datastreams
            LOGGER.error("Error fetching data stream mappings", e);
        }
    }

    private static void mergeProperties(Map<String, MappedType> result, String type, Map<String, Object> mappings, Map<String, Object> properties, String index)
    {
        result.compute(type, (k, v) ->
        {
            MappedType r;
            Map<QualifiedName, MappedProperty> typeResult;

            if (v != null)
            {
                typeResult = v.properties;
                r = v;
            }
            else
            {
                typeResult = new HashMap<>();
                r = new MappedType(mappings, typeResult);
            }

            populateAnalyzedFields(typeResult, properties, false, null, index);
            return r;
        });
    }

    private static void populateAnalyzedFields(Map<QualifiedName, MappedProperty> result, Map<String, Object> properties, boolean nested, QualifiedName parentKey, String index)
    {
        for (Entry<String, Object> entry : properties.entrySet())
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMap = (Map<String, Object>) entry.getValue();
            @SuppressWarnings("unchecked")
            Map<String, Object> subProperties = (Map<String, Object>) propertiesMap.get("properties");

            QualifiedName field = parentKey != null ? parentKey.extend(entry.getKey())
                    : QualifiedName.of(entry.getKey());

            if (subProperties != null)
            {
                boolean isNested = nested
                        || "nested".equals(propertiesMap.get("type"));
                populateAnalyzedFields(result, subProperties, isNested, field, index);
                continue;
            }

            QualifiedName nestedPath = nested ? parentKey
                    : null;

            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) propertiesMap.get("fields");
            List<MappedProperty> fieldsMappedProperties = new ArrayList<>();

            if (fields != null)
            {
                // Only traverse one level of fields (don't know if there can be multiple levels)
                for (Entry<String, Object> e : fields.entrySet())
                {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProperties = (Map<String, Object>) e.getValue();
                    QualifiedName fieldName = field.extend(e.getKey());
                    fieldsMappedProperties.add(createMappedProperty(fieldName, fieldProperties, emptyList(), nestedPath, emptyMap()));
                }
            }

            propertiesMap.put("nested", nested);

            QualifiedName lowerField = field.toLowerCase();

            MappedProperty mappedProperty = result.get(lowerField);
            List<String> indices = emptyList();
            if (mappedProperty != null)
            {
                indices = mappedProperty.indices;
            }
            mappedProperty = createMappedProperty(field, propertiesMap, fieldsMappedProperties, nestedPath, propertiesMap);
            mappedProperty.indices.addAll(indices);
            mappedProperty.indices.add(index);
            result.put(lowerField, mappedProperty);
        }
    }

    private static MappedProperty createMappedProperty(QualifiedName name, Map<String, Object> propertiesMap, List<MappedProperty> fields, QualifiedName nestedPath, Map<String, Object> meta)
    {
        String type = (String) propertiesMap.get("type");
        Object index = propertiesMap.get("index");
        return new MappedProperty(name, type, index, nestedPath, fields, meta);
    }

    @SuppressWarnings("resource")
    private static <T> T get(Class<T> clazz, IQuerySession session, String catalogAlias, String endpoint, String path)
    {
        HttpGet get = new HttpGet(String.format("%s/%s", endpoint, path));
        try
        {
            return HttpClientUtils.execute(session, catalogAlias, get, response ->
            {
                HttpEntity entity = response.getEntity();
                if (response.getCode() != HttpStatus.SC_OK)
                {
                    throw new RuntimeException("Error query Elastic: " + IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8));
                }

                return ESDatasource.MAPPER.readValue(entity.getContent(), clazz);
            });

        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error query Elastic at: " + endpoint, e);
        }
    }

    /** Class containing info about a type */
    static class MappedType
    {
        final Map<String, Object> meta;
        final Map<QualifiedName, MappedProperty> properties;

        MappedType(Map<String, Object> meta, Map<QualifiedName, MappedProperty> properties)
        {
            this.meta = meta;
            this.properties = properties;
        }
    }

    /** Class containing info about a mapped property such as type, ev. fields, analyzed etc. */
    static class MappedProperty
    {
        private static final Set<String> NON_QUOTE_TYPES = new HashSet<>(asList("boolean", "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long"));

        final QualifiedName name;
        final String type;
        final Object index;
        final List<MappedProperty> fields;
        final QualifiedName nestedPath;
        final Map<String, Object> meta;
        final List<String> indices = new ArrayList<>();

        MappedProperty(QualifiedName name, String type, Object index, QualifiedName nestedPath, List<MappedProperty> fields, Map<String, Object> meta)
        {
            this.name = requireNonNull(name, "name");
            this.type = requireNonNull(type, "type");
            this.index = index;
            this.nestedPath = nestedPath;
            this.fields = requireNonNull(fields, "fields");
            this.meta = requireNonNull(meta, "meta");
        }

        /**
         * Returns true if this mapped property is free text mapping. ie. the value is analyzed and tokenized
         */
        boolean isFreeTextMapping()
        {
            // Old ES version style
            return ("string".equals(type)
                    && !"not_analyzed".equals(index))
                    // New ES version style
                    || "text".equals(type);
        }

        /** Returns true if this is a string (non freetext) type of mapping. */
        boolean isStringType()
        {
            return "string".equalsIgnoreCase(type)
                    || "keyword".equalsIgnoreCase(type);
        }

        /** Returns true if this property values should be quoted when used in queries */
        boolean shouldQuoteValues()
        {
            return !NON_QUOTE_TYPES.contains(type);
        }

        /**
         * Searches among mapped fields for first non freetext property. Used when the top level field is freetext and a non freetext field is wanted for a EQUAL comparison etc.
         */
        MappedProperty getNonFreeTextField()
        {
            if (fields == null)
            {
                return null;
            }
            return fields.stream()
                    .filter(f -> !f.isFreeTextMapping())
                    .findFirst()
                    .orElse(null);
        }

        static MappedProperty of(QualifiedName name, String type)
        {
            return new MappedProperty(name, type, null, null, emptyList(), emptyMap());
        }

        static MappedProperty of(QualifiedName name, String type, QualifiedName nestedPath)
        {
            return new MappedProperty(name, type, null, nestedPath, emptyList(), emptyMap());
        }
    }
}
