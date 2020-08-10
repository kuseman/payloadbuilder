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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.kuse.payloadbuilder.catalog.es.ESOperator.EsType;
import org.kuse.payloadbuilder.catalog.es.ESOperator.PropertyPredicate;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Catalog for querying elastic search */
class ESCatalog extends Catalog
{
    public static final String NAME = "Elastic search";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String INDEX_KEY = "index";

    ESCatalog()
    {
        super("EsCatalog");
        registerFunction(new MustacheCompileFunction(this));
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
            Map<String, Object> map = MAPPER.readValue(response.getEntity().getContent(), Map.class);
            map = (Map<String, Object>) map.get(index);
            map = (Map<String, Object>) map.get("mappings");
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
        // All tables have a doc id index
        return asList(new Index(table, asList(ESOperator.DOCID), 250));
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
            if (key != null)
            {
                propertyPredicates.add(new PropertyPredicate(tableAlias.getAlias(), defaultIfBlank(key, column), pair));
                it.remove();
            }
        }
    }

    private List<Pair<String, String>> collectSortItems(
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
            
            String key = analyzedProperties.get(column);
            if (key == null)
            {
                return emptyList();
            }

            if (result == null)
            {
                result = new ArrayList<>();
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
        HttpGet getMappings = new HttpGet(String.format("%s/%s/%s/_mapping", esType.endpoint, esType.index, esType.type));
        try (CloseableHttpResponse response = CLIENT.execute(getMappings))
        {
            if (response.getStatusLine().getStatusCode() != 200)
            {
                return emptyMap();
            }

            Map<String, Object> map = MAPPER.readValue(response.getEntity().getContent(), Map.class);
            map = Optional.of(map)
                    .map(m -> (Map<String, Object>) m.get(esType.index))
                    .map(m -> (Map<String, Object>) m.get("mappings"))
                    .map(m -> (Map<String, Object>) m.get(esType.type))
                    .map(m -> (Map<String, Object>) m.get("properties"))
                    .orElse(null);

            if (map == null)
            {
                return emptyMap();
            }

            Map<String, String> result = new HashMap<>();
            populateAnalyzedFields(result, map, null);
            return result;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error fetching mappings from " + esType.endpoint, e);
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
            
            // Collected nested properties one level down only
            if (parentKey == null && subProperties != null)
            {
                populateAnalyzedFields(result, subProperties, entry.getKey());
                continue;
            }

            String index = (String) propertiesMap.get("index");

            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) propertiesMap.get("fields");
            
            // if index is null and there exists a field that is not_analyzed pick that one
            if (index == null && fields != null)
            {
                boolean added = false;
                for (Entry<String, Object> e : fields.entrySet())
                {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProperties = (Map<String, Object>) e.getValue();
                    String fieldIndex = (String) fieldProperties.get("index");
                    if ("not_analyzed".equals(fieldIndex))
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
            
            // - Type should no be nested/object
            //   Not supported to filter on
            // - index cannot be NO
            if ((index == null || "not_analyzed".equals(index)))
            {
                result.put((parentKey != null ? parentKey + "." : "") + entry.getKey(), "");
            }
        }
    }
}
