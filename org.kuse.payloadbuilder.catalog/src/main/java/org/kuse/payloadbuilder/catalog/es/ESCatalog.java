package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.CLIENT;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.MAPPER;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.kuse.payloadbuilder.catalog.es.ESUtils.SortItemMeta;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.SortItem;

/** Catalog for querying elastic search */
public class ESCatalog extends Catalog
{
    public static final String NAME = "Elastic search";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String INDEX_KEY = "index";
    private static final String CACHE_MAPPINGS_TTL = "cache.mappings.ttl";
    /** Default cache time for mappings */
    public static final int MAPPINGS_CACHE_TTL = 60;
    static final String SINGLE_TYPE_TABLE_NAME = "_doc";
    private static final int BATCH_SIZE = 250;

    public ESCatalog()
    {
        super("EsCatalog");
        registerFunction(new MustacheCompileFunction(this));
        registerFunction(new SearchFunction(this));
        registerFunction(new MatchFunction(this));
        registerFunction(new QueryFunction(this));
        registerFunction(new CatFunction(this));
    }

    @Override
    public Operator getSystemOperator(OperatorData data)
    {
        final QuerySession session = data.getSession();
        final String catalogAlias = data.getCatalogAlias();
        final TableAlias alias = data.getTableAlias();
        QualifiedName table = alias.getTable();

        if (table.size() == 1)
        {
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getTablesIterator(session, catalogAlias, alias));
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getColumnsIterator(session, catalogAlias, alias));
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return getFunctionsOperator(data.getNodeId(), alias);
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getIndicesIterator(session, catalogAlias, alias));
            }
        }

        throw new RuntimeException(table + " is not supported");
    }

    @Override
    public Operator getScanOperator(OperatorData data)
    {
        return getIndexOperator(data, null);
    }

    @Override
    public Operator getIndexOperator(OperatorData data, Index index)
    {
        ESType esType = ESType.of(data.getSession(), data.getCatalogAlias(), data.getTableAlias().getTable());
        List<PropertyPredicate> propertyPredicates = emptyList();
        List<SortItemMeta> sortItems = emptyList();
        MappedProperty indexProperty = null;

        // Fetch analyzed properties
        Map<String, MappedType> mappedTypes = getProperties(data.getSession(), data.getCatalogAlias(), esType.endpoint, esType.index);
        Map<String, MappedProperty> properties = Optional.ofNullable(mappedTypes.get(esType.type))
                .map(m -> m.properties)
                .orElse(emptyMap());
        if (index != null)
        {
            if (index.getColumns().size() != 1)
            {
                throw new IllegalArgumentException("Invalid index, catalog only supports single column indices");
            }

            String indexColumn = index.getColumns().get(0);
            // Fetch mapped property for index column. Not needed for _id, _parent_id
            if (!(ESOperator.DOCID.equalsIgnoreCase(indexColumn)
                || ESOperator.PARENTID.equalsIgnoreCase(indexColumn)))
            {
                indexProperty = properties.get(indexColumn);
                //CSOFF
                if (indexProperty == null)
                //CSON
                {
                    throw new IllegalArgumentException("Invalid index column: " + indexColumn);
                }
            }
        }

        if (!data.getPredicatePairs().isEmpty())
        {
            propertyPredicates = new ArrayList<>();
            collectPredicates(
                    data.getTableAlias(),
                    data.getPredicatePairs(),
                    properties,
                    propertyPredicates);
        }

        if (!data.getSortItems().isEmpty())
        {
            sortItems = collectSortItems(data.getTableAlias(), properties, data.getSortItems());
        }

        return new ESOperator(
                data.getNodeId(),
                data.getCatalogAlias(),
                data.getTableAlias(),
                index,
                indexProperty,
                propertyPredicates,
                sortItems);
    };

    private void collectPredicates(
            TableAlias tableAlias,
            List<AnalyzePair> predicatePairs,
            Map<String, MappedProperty> properties,
            List<PropertyPredicate> propertyPredicates)
    {
        Iterator<AnalyzePair> it = predicatePairs.iterator();
        while (it.hasNext())
        {
            AnalyzePair pair = it.next();
            if (pair.getType() == Type.UNDEFINED || pair.getType() == Type.NOT_NULL || pair.getType() == Type.NULL)
            {
                // TODO: analyze function arguments to properly find a field that is searchable
                // ie. ESC mapping for: http.request.body.content
                //     has a field ".text" with type text that should
                //     be used in full text search instead
                if (isFullTextSearchPredicate(pair.getPredicate()))
                {
                    propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "", pair, true));
                    it.remove();
                }

                continue;
            }

            QualifiedName qname = pair.getQname(tableAlias.getAlias());
            if (qname == null)
            {
                continue;
            }

            String column = qname.toDotDelimited();
            MappedProperty property = properties.get(column);
            // Extra columns only support EQUALS
            if (ESOperator.INDEX.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_index", pair, false));
                it.remove();
            }
            else if (ESOperator.TYPE.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_type", pair, false));
                it.remove();
            }
            else if (ESOperator.DOCID.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_id", pair, false));
                it.remove();
            }
            else if (ESOperator.PARENTID.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_parent", pair, false));
                it.remove();
            }
            // TODO: strings only support equals
            else if (property != null)
            {
                String field = property.name;
                if (property.isFreeTextMapping())
                {
                    property = property.getNonFreeTextField();
                    //CSOFF
                    if (property == null)
                    //CSON
                    {
                        continue;
                    }
                    field = property.name;
                }

                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), field, property.nestedPath, pair, false));
                it.remove();
            }
        }
    }

    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        ESType esType = ESType.of(session, catalogAlias, table);
        // All indexed non-free-text fields are index candidates
        Map<String, MappedType> mappedTypes = getProperties(session, catalogAlias, esType.endpoint, esType.index);
        Map<String, MappedProperty> properties = Optional.of(mappedTypes.get(esType.type))
                .map(m -> m.properties)
                .orElse(emptyMap());
        return getIndices(table, properties);
    }

    private <T> Map<String, Object> get(String endpoint, String index, String path)
    {
        HttpEntity entity = null;
        HttpGet getAlias = new HttpGet(String.format("%s/%s/%s", endpoint, index, path));
        try (CloseableHttpResponse response = CLIENT.execute(getAlias))
        {
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
            {
                throw new RuntimeException("Error query Elastic: " + IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8));
            }
            return MAPPER.readValue(entity.getContent(), Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error query Elastic at: " + endpoint, e);
        }
        finally
        {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private boolean isFullTextSearchPredicate(Expression expression)
    {
        if (!(expression instanceof QualifiedFunctionCallExpression))
        {
            return false;
        }
        ScalarFunctionInfo functionInfo = ((QualifiedFunctionCallExpression) expression).getFunctionInfo();
        Class<?> clazz = functionInfo.getClass();
        return clazz == MatchFunction.class || clazz == QueryFunction.class;
    }

    //CSOFF
    private List<SortItemMeta> collectSortItems(
            //CSON
            TableAlias tableAlias,
            Map<String, MappedProperty> properties,
            List<SortItem> sortItems)
    {
        List<SortItemMeta> result = null;
        for (SortItem sortItem : sortItems)
        {
            if (!(sortItem.getExpression() instanceof QualifiedReferenceExpression))
            {
                return emptyList();
            }

            QualifiedReferenceExpression qe = (QualifiedReferenceExpression) sortItem.getExpression();
            QualifiedName qname = qe.getQname();

            String column;
            // Remove alias from qname
            if (Objects.equals(tableAlias.getAlias(), qname.getAlias()))
            {
                column = qname.extract(1).toDotDelimited();
            }
            else
            {
                column = qname.toDotDelimited();
            }

            if (result == null)
            {
                result = new ArrayList<>();
            }

            if (ESOperator.PARENTID.equals(column))
            {
                result.add(new SortItemMeta(MappedProperty.of("_parent", "string"), sortItem.getOrder(), sortItem.getNullOrder()));
                continue;
            }
            else if (ESOperator.INDEX.equals(column))
            {
                result.add(new SortItemMeta(MappedProperty.of("_index", "string"), sortItem.getOrder(), sortItem.getNullOrder()));
                continue;
            }
            else if (ESOperator.DOCID.equals(column))
            {
                // Use _uid here since sorting on _id is not supported without extra indexing
                result.add(new SortItemMeta(MappedProperty.of("_uid", "string"), sortItem.getOrder(), sortItem.getNullOrder()));
                continue;
            }

            MappedProperty property = properties.get(column);
            if (property == null)
            {
                return emptyList();
            }

            result.add(new SortItemMeta(property, sortItem.getOrder(), sortItem.getNullOrder()));
        }

        // Consume items from framework
        sortItems.clear();
        return result;
    }

    /** Returns properties for current sessions endpoint/index */
    @SuppressWarnings("unchecked")
    private Map<String, MappedType> getProperties(QuerySession session, String catalogAlias, String endpoint, String index)
    {
        if (isBlank(endpoint) || isBlank(index))
        {
            throw new IllegalArgumentException("Missing endpoint/index in catalog properties.");
        }
        Integer ttl = session.getCatalogProperty(catalogAlias, CACHE_MAPPINGS_TTL);
        QualifiedName cacheName = QualifiedName.of(NAME, endpoint, index);
        return session.getCustomCacheProvider().computIfAbsent(cacheName, "mappings", Duration.ofMinutes(ttl != null ? ttl : MAPPINGS_CACHE_TTL), () ->
        {
            Map<String, Object> map = get(endpoint, index, "_mappings");
            Map<String, MappedType> result = new HashMap<>();

            // Traverse all indices matching mappings
            // if this is a multi indices query like wildcard (*) or
            // comma separated then we will have multiple indices to fetch properties
            // for and should be merged
            for (Object obj : map.values())
            {
                Map<String, Object> currentIndexMappings = (Map<String, Object>) obj;
                Map<String, Object> mappings = (Map<String, Object>) currentIndexMappings.get("mappings");
                Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

                // Single type ES version, return dummy table_name
                if (properties != null && !properties.containsKey("properties"))
                {
                    mergeProperties(result, SINGLE_TYPE_TABLE_NAME, mappings, properties);
                }
                else
                {
                    // Old es version with types
                    for (Entry<String, Object> e : mappings.entrySet())
                    {
                        Map<String, Object> currentMappings = (Map<String, Object>) e.getValue();
                        properties = (Map<String, Object>) currentMappings.get("properties");
                        mergeProperties(result, e.getKey(), currentMappings, properties);
                    }
                }
            }

            return result;
        });
    }

    private void mergeProperties(
            Map<String, MappedType> result,
            String type,
            Map<String, Object> mappings,
            Map<String, Object> properties)
    {
        result.compute(type, (k, v) ->
        {
            MappedType r;
            Map<String, MappedProperty> typeResult;

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

            populateAnalyzedFields(typeResult, properties, false, null);
            return r;
        });
    }

    private void populateAnalyzedFields(Map<String, MappedProperty> result, Map<String, Object> properties, boolean nested, String parentKey)
    {
        for (Entry<String, Object> entry : properties.entrySet())
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMap = (Map<String, Object>) entry.getValue();
            @SuppressWarnings("unchecked")
            Map<String, Object> subProperties = (Map<String, Object>) propertiesMap.get("properties");

            String field = (parentKey == null ? "" : parentKey + ".") + entry.getKey();
            if (subProperties != null)
            {
                boolean isNested = nested || "nested".equals(propertiesMap.get("type"));
                populateAnalyzedFields(result, subProperties, isNested, field);
                continue;
            }

            String nestedPath = nested ? parentKey : null;

            List<MappedProperty> fieldsMappedProperties = emptyList();
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) propertiesMap.get("fields");

            if (fields != null)
            {
                fieldsMappedProperties = new ArrayList<>(fields.size());
                // Only traverse one level of fields (don't know if there can be multiple levels)
                for (Entry<String, Object> e : fields.entrySet())
                {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProperties = (Map<String, Object>) e.getValue();
                    String fieldName = field + "." + e.getKey();
                    fieldsMappedProperties.add(create(fieldName, fieldProperties, emptyList(), nestedPath, emptyMap()));
                }
            }

            propertiesMap.put("nested", nested);
            MappedProperty mappedProperty = create(field, propertiesMap, fieldsMappedProperties, nestedPath, propertiesMap);
            result.put(field, mappedProperty);
        }
    }

    private MappedProperty create(
            String name,
            Map<String, Object> propertiesMap,
            List<MappedProperty> fields,
            String nestedPath,
            Map<String, Object> meta)
    {
        String type = (String) propertiesMap.get("type");
        Object index = propertiesMap.get("index");
        return new MappedProperty(name, type, index, nestedPath, fields, meta);
    }

    private TupleIterator getTablesIterator(QuerySession session, String catalogAlias, TableAlias tableAlias)
    {
        String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> types = getProperties(session, catalogAlias, endpoint, index);

        // Collect result and columns
        Set<String> columns = new LinkedHashSet<>();
        List<Map<String, Object>> result = new ArrayList<>(types.size());
        for (Entry<String, MappedType> e : types.entrySet())
        {
            Map<String, Object> meta = new LinkedHashMap<>();
            // Make sure we have a name first
            meta.put(SYS_TABLES_NAME, e.getKey());
            meta.putAll(e.getValue().meta);
            // Make sure name is not overwritten by meta
            meta.put(SYS_TABLES_NAME, e.getKey());
            columns.addAll(meta.keySet());
            result.add(meta);
        }

        String[] columnsArray = columns.toArray(EMPTY_STRING_ARRAY);
        return TupleIterator.wrap(result.stream().map(map ->
        {
            return (Tuple) Row.of(tableAlias, columnsArray, new Row.MapValues(map, columnsArray));
        }).iterator());
    }

    private TupleIterator getColumnsIterator(QuerySession session, String catalogAlias, TableAlias tableAlias)
    {
        final List<Map<String, Object>> result = new ArrayList<>();
        final String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        final String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> types = getProperties(session, catalogAlias, endpoint, index);

        Set<String> columns = new LinkedHashSet<>();
        for (Entry<String, MappedType> e : types.entrySet())
        {
            for (MappedProperty prop : e.getValue().properties.values())
            {
                Map<String, Object> meta = new LinkedHashMap<>();
                meta.put(SYS_COLUMNS_TABLE, e.getKey());
                meta.put(SYS_COLUMNS_NAME, prop.name);

                Map<String, Object> tmp = new HashMap<>(prop.meta);
                Object value = tmp.remove(SYS_COLUMNS_TABLE);
                if (value != null)
                {
                    meta.put("_table", value);
                }
                value = tmp.remove(SYS_COLUMNS_NAME);
                if (value != null)
                {
                    meta.put("_name", value);
                }
                meta.putAll(tmp);
                columns.addAll(meta.keySet());
                result.add(meta);
            }
        }

        Comparator<Map<String, Object>> comparator = Comparator.comparing(c -> (String) c.get(SYS_COLUMNS_TABLE));
        comparator = comparator.thenComparing(c -> (String) c.get(SYS_COLUMNS_NAME));
        Collections.sort(result, comparator);

        String[] columnsArray = columns.toArray(EMPTY_STRING_ARRAY);
        return TupleIterator.wrap(result.stream().map(map ->
        {
            return (Tuple) Row.of(tableAlias, columnsArray, new Row.MapValues(map, columnsArray));
        }).iterator());
    }

    private TupleIterator getIndicesIterator(QuerySession session, String catalogAlias, TableAlias tableAlias)
    {
        final String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        final String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> properties = getProperties(session, catalogAlias, endpoint, index);
        String[] columns = new String[] {SYS_INDICES_TABLE, SYS_INDICES_COLUMNS};
        return TupleIterator.wrap(properties
                .entrySet()
                .stream()
                .flatMap(e -> getIndices(QualifiedName.of(e.getKey()), e.getValue().properties).stream())
                .map(i -> (Tuple) Row.of(tableAlias, columns, new Object[] {i.getTable().getLast(), i.getColumns()}))
                .iterator());
    }

    private List<Index> getIndices(QualifiedName table, Map<String, MappedProperty> properties)
    {
        List<Index> result = new ArrayList<>(2 + properties.size());
        // All tables have a doc id index
        result.add(new Index(table, singletonList(ESOperator.DOCID), BATCH_SIZE));
        result.add(new Index(table, singletonList(ESOperator.PARENTID), BATCH_SIZE));

        for (MappedProperty p : properties.values())
        {
            // Nested fields not supported at the moment
            if (p.nestedPath != null)
            {
                continue;
            }

            String field = p.name;
            // Free text mappings cannot be used as index column
            // See if there exists another mapping
            if (p.isFreeTextMapping() && p.getNonFreeTextField() == null)
            {
                continue;
            }

            result.add(new Index(table, singletonList(field), BATCH_SIZE));
        }

        return result;
    }

    /** Class containing info about a type */
    static class MappedType
    {
        final Map<String, Object> meta;
        final Map<String, MappedProperty> properties;

        MappedType(Map<String, Object> meta, Map<String, MappedProperty> properties)
        {
            this.meta = meta;
            this.properties = properties;
        }
    }

    /** Class containing info about a mapped property such as type, ev. fields, analyzed etc. */
    static class MappedProperty
    {
        private static final Set<String> NON_QUOTE_TYPES = new HashSet<>(asList(
                "boolean",
                "long",
                "integer",
                "short",
                "byte",
                "double",
                "float",
                "half_float",
                "scaled_float",
                "unsigned_long"));

        final String name;
        final String type;
        final Object index;
        final List<MappedProperty> fields;
        final String nestedPath;
        final Map<String, Object> meta;

        MappedProperty(
                String name,
                String type,
                Object index,
                String nestedPath,
                List<MappedProperty> fields,
                Map<String, Object> meta)
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
                || ("text".equals(type));
        }

        /** Returns true if this property values should be quoted when used in queries */
        boolean shouldQuoteValues()
        {
            return !NON_QUOTE_TYPES.contains(type);
        }

        /**
         * Searches among mapped fields for first non freetext property. Used when the top level field is freetext and a non freetext field is wanted
         * for a EQUAL comparison etc.
         */
        MappedProperty getNonFreeTextField()
        {
            if (fields == null)
            {
                return null;
            }
            return fields
                    .stream()
                    .filter(f -> !f.isFreeTextMapping())
                    .findFirst()
                    .orElse(null);
        }

        static MappedProperty of(String name, String type)
        {
            return new MappedProperty(name, type, null, null, emptyList(), emptyMap());
        }
    }
}
