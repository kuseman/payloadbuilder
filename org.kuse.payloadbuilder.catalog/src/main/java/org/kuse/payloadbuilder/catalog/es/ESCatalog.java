package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.CLIENT;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.MAPPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.kuse.payloadbuilder.catalog.es.ESOperator.EsType;
import org.kuse.payloadbuilder.catalog.es.ESOperator.PropertyPredicate;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Catalog for querying elastic search */
public class ESCatalog extends Catalog
{
    public static final String NAME = "Elastic search";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String INDEX_KEY = "index";
    static final String SINGLE_TYPE_TABLE_NAME = "_doc";

    public ESCatalog()
    {
        super("EsCatalog");
        registerFunction(new MustacheCompileFunction(this));
        registerFunction(new SearchFunction(this));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getTables(QuerySession session, String catalogAlias)
    {
        String endpoint = (String) session.getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
        String index = (String) session.getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY);

        if (isBlank(endpoint) || isBlank(index))
        {
            throw new IllegalArgumentException("Missing endpoint/index in catalog properties.");
        }

        HttpGet getMappings = new HttpGet(String.format("%s/%s/_mapping", endpoint, index));
        try (CloseableHttpResponse response = CLIENT.execute(getMappings))
        {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
            {
                throw new RuntimeException("Error query Elastic for mappings." + IOUtils.toString(response.getEntity().getContent()));
            }

            Map<String, Object> map = MAPPER.readValue(response.getEntity().getContent(), Map.class);
            map = (Map<String, Object>) map.get(index);
            map = (Map<String, Object>) map.get("mappings");

            Map<String, Object> properties = (Map<String, Object>) map.get("properties");

            // Single type ES version, return dummy table_name
            if (properties != null && !properties.containsKey("properties"))
            {
                return asList(SINGLE_TYPE_TABLE_NAME);
            }

            return new ArrayList<>(map.keySet());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error querying " + endpoint, e);
        }
    }

    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        // No index support on multi index queries
        String indexString = table.toString();
        if (indexString.contains(",") || indexString.contains("*"))
        {
            return emptyList();
        }
        // All tables have a doc id index
        return asList(new IdIndex(table), new ParentIndex(table));
    }

    @Override
    public Operator getScanOperator(OperatorData data)
    {
        List<PropertyPredicate> propertyPredicates = emptyList();
        List<Pair<String, String>> sortItems = emptyList();
        // Extract potential predicate to send to ES
        if (!data.getPredicatePairs().isEmpty() || !data.getSortItems().isEmpty())
        {
            // Fetch analyzed properties
            Map<String, String> analyzedFields = getAnalyzedPropertyNames(data.getSession(), data.getCatalogAlias(), data.getTableAlias().getTable());

            if (!data.getPredicatePairs().isEmpty())
            {
                propertyPredicates = new ArrayList<>();
                collectPredicates(data.getTableAlias(), data.getPredicatePairs(), analyzedFields, propertyPredicates);
            }

            if (!data.getSortItems().isEmpty())
            {
                sortItems = collectSortItems(data.getTableAlias(), analyzedFields, data.getSortItems());
            }
        }

        return new ESOperator(
                data.getNodeId(),
                data.getCatalogAlias(),
                data.getTableAlias(),
                null,
                propertyPredicates,
                sortItems);
    }

    @Override
    public Operator getIndexOperator(OperatorData data, Index index)
    {
        // TODO: if index is of ParentId-type then push down of predicate is possible
        return new ESOperator(
                data.getNodeId(),
                data.getCatalogAlias(),
                data.getTableAlias(),
                index,
                emptyList(),
                emptyList());
    };

    private void collectPredicates(
            TableAlias tableAlias,
            List<AnalyzePair> predicatePairs,
            Map<String, String> analyzedProperties,
            List<ESOperator.PropertyPredicate> propertyPredicates)
    {
        Iterator<AnalyzePair> it = predicatePairs.iterator();
        while (it.hasNext())
        {
            AnalyzePair pair = it.next();
            if (pair.getType() == Type.UNDEFINED)
            {
                continue;
            }

            QualifiedName qname = pair.getQname(tableAlias.getAlias());
            if (qname == null)
            {
                continue;
            }

            String column = qname.toString();
            String key = analyzedProperties.get(column);
            // Extra columns only support EQUALS
            if (ESOperator.INDEX.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_index", pair));
                it.remove();
            }
            else if (ESOperator.TYPE.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_type", pair));
                it.remove();
            }
            else if (ESOperator.DOCID.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_id", pair));
                it.remove();
            }
            else if (ESOperator.PARENTID.equals(column) && pair.getComparisonType() == ComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), "_parent", pair));
                it.remove();
            }
            // TODO: strings only support equals
            else if (key != null)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), defaultIfBlank(key, column), pair));
                it.remove();
            }
        }
    }

    //CSOFF
    private List<Pair<String, String>> collectSortItems(
            //CSON
            TableAlias tableAlias,
            Map<String, String> analyzedProperties,
            List<SortItem> sortItems)
    {
        List<Pair<String, String>> result = null;
        for (SortItem sortItem : sortItems)
        {
            if (sortItem.getNullOrder() != NullOrder.UNDEFINED)
            {
                return emptyList();
            }
            else if (!(sortItem.getExpression() instanceof QualifiedReferenceExpression))
            {
                return emptyList();
            }

            QualifiedReferenceExpression qe = (QualifiedReferenceExpression) sortItem.getExpression();
            QualifiedName qname = qe.getQname();

            String column;
            // Remove alias from qname
            if (Objects.equals(tableAlias.getAlias(), qname.getAlias()))
            {
                column = qname.extract(1).toString();
            }
            else
            {
                column = qname.toString();
            }

            if (result == null)
            {
                result = new ArrayList<>();
            }

            if (ESOperator.PARENTID.equals(column))
            {
                result.add(Pair.of("_parent", sortItem.getOrder() == Order.ASC ? "asc" : "desc"));
                continue;
            }
            else if (ESOperator.INDEX.equals(column))
            {
                result.add(Pair.of("_index", sortItem.getOrder() == Order.ASC ? "asc" : "desc"));
                continue;
            }
            else if (ESOperator.DOCID.equals(column))
            {
                result.add(Pair.of("_id", sortItem.getOrder() == Order.ASC ? "asc" : "desc"));
                continue;
            }

            String key = analyzedProperties.get(column);

            if (key == null)
            {
                return emptyList();
            }

            result.add(Pair.of(defaultIfBlank(key, column), sortItem.getOrder() == Order.ASC ? "asc" : "desc"));
        }

        // Consume items from framework
        sortItems.clear();
        return result;
    }

    /** Returns mapping for provided table */
    @SuppressWarnings("unchecked")
    private Map<String, String> getAnalyzedPropertyNames(QuerySession session, String catalogAlias, QualifiedName table)
    {
        EsType esType = EsType.of(session, catalogAlias, table);
        final boolean isSingleType = SINGLE_TYPE_TABLE_NAME.equals(esType.type);
        HttpGet getMappings = new HttpGet(String.format("%s/%s/%s_mapping", esType.endpoint, esType.index, isSingleType ? "" : esType.type + "/"));
        try (CloseableHttpResponse response = CLIENT.execute(getMappings))
        {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
            {
                return emptyMap();
            }

            Map<String, Object> map = MAPPER.readValue(response.getEntity().getContent(), Map.class);
            if (map == null)
            {
                return emptyMap();
            }
            Map<String, String> result = new HashMap<>();
            // Loop all indices in mappings result
            for (Entry<String, Object> e : map.entrySet())
            {
                Optional.of((Map<String, Object>) e.getValue())
                        .map(m -> (Map<String, Object>) m.get("mappings"))
                        .map(m -> isSingleType ? m : (Map<String, Object>) m.get(esType.type))
                        .map(m -> (Map<String, Object>) m.get("properties"))
                        .ifPresent(m -> populateAnalyzedFields(result, m, null));
            }
            return result;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error fetching mappings from " + esType.endpoint, e);
        }
    }

    /** Maker class for _id index */
    static class IdIndex extends Index
    {
        private static final int BATCH_SIZE = 250;

        IdIndex(QualifiedName table)
        {
            super(table, asList(ESOperator.DOCID), BATCH_SIZE);
        }
    }

    /** Maker class for _parent index */
    static class ParentIndex extends Index
    {
        private static final int BATCH_SIZE = 250;

        ParentIndex(QualifiedName table)
        {
            super(table, asList(ESOperator.PARENTID), BATCH_SIZE);
        }
    }

    private void populateAnalyzedFields(Map<String, String> result, Map<String, Object> properties, String parentKey)
    {
        for (Entry<String, Object> entry : properties.entrySet())
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMap = (Map<String, Object>) entry.getValue();
            @SuppressWarnings("unchecked")
            Map<String, Object> subProperties = (Map<String, Object>) propertiesMap.get("properties");

            if (subProperties != null)
            {
                populateAnalyzedFields(result, subProperties, (parentKey == null ? "" : parentKey + ".") + entry.getKey());
                continue;
            }

            Object index = propertiesMap.get("index");
            String type = (String) propertiesMap.get("type");

            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) propertiesMap.get("fields");

            // if index is null and there exists a field that is not_analyzed pick that one
            if (index == null && fields != null && ("string".equals(type) || "text".equals(type)))
            {
                boolean added = false;
                for (Entry<String, Object> e : fields.entrySet())
                {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProperties = (Map<String, Object>) e.getValue();
                    String fieldIndex = (String) fieldProperties.get("index");
                    String fieldType = (String) fieldProperties.get("type");
                    if ("not_analyzed".equals(fieldIndex) || "keyword".equals(fieldType))
                    {
                        String property = (parentKey != null ? parentKey + "." : "") + entry.getKey();
                        result.put(property, property + "." + e.getKey());
                        added = true;
                        break;
                    }
                }
                if (added)
                {
                    continue;
                }
            }

            if (index == null || "not_analyzed".equals(index))
            {
                result.put((parentKey != null ? parentKey + "." : "") + entry.getKey(), "");
            }
        }
    }
}
