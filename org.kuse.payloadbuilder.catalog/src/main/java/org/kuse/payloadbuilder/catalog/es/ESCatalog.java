package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.CLIENT;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.MAPPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.kuse.payloadbuilder.catalog.es.ESOperator.EsType;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.TableOption;

/** Catalog for querying elastic search */
class ESCatalog extends Catalog
{
    public static final String NAME = "Elastic search";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String INDEX_KEY = "index";

    ESCatalog()
    {
        super("EsCatalog");
    }

    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        // All tables have a doc id index
        return asList(new Index(table, asList(ESOperator.DOCID), 250));
    }

    @Override
    public Operator getScanOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            TablePredicate predicate,
            List<TableOption> tableOptions)
    {
        List<Pair<String, Expression>> fieldPredicates = emptyList();
        // Extract potential predicate to send to ES
        if (predicate.getPredicate() != null)
        {
            fieldPredicates = new ArrayList<>();
            List<String> names = getAnalyzedPropertyNames(session, catalogAlias, tableAlias.getTable());
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(predicate.getPredicate());
            List<AnalyzePair> leftOvers = new ArrayList<>();
            for (AnalyzePair pair : analyzeResult.getPairs())
            {
                String column = pair.getColumn(tableAlias.getAlias(), true);
                if (names.contains(column))
                {
                    Pair<Expression, Expression> expressionPair = pair.getExpressionPair(tableAlias.getAlias(), true);
                    fieldPredicates.add(Pair.of(column, expressionPair.getRight()));
                }
                else
                {
                    leftOvers.add(pair);
                }
            }
            predicate.setPredicate(new AnalyzeResult(leftOvers).getPredicate());
        }

        return new ESOperator(nodeId, catalogAlias, tableAlias, null, fieldPredicates);
    }

    @Override
    public Operator getIndexOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index,
            TablePredicate predicate,
            List<TableOption> tableOptions)
    {
        return new ESOperator(nodeId, catalogAlias, tableAlias, index, emptyList());
    };

    /** Returns mapping for provided table */
    @SuppressWarnings("unchecked")
    private List<String> getAnalyzedPropertyNames(QuerySession session, String catalogAlias, QualifiedName table)
    {
        EsType esType = EsType.of(session, catalogAlias, table);
        HttpGet getMappings = new HttpGet(String.format("%s/%s/%s/_mapping", esType.endpoint, esType.index, esType.type));
        try (CloseableHttpResponse response = CLIENT.execute(getMappings))
        {
            Map<String, Object> map = MAPPER.readValue(response.getEntity().getContent(), Map.class);
            map = (Map<String, Object>) map.get(esType.index);
            map = (Map<String, Object>) map.get("mappings");
            map = (Map<String, Object>) map.get(esType.type);
            map = (Map<String, Object>) map.get("properties");

            List<String> result = new ArrayList<>();
            for (Entry<String, Object> entry : map.entrySet())
            {
                Map<String, Object> properties = (Map<String, Object>) entry.getValue();
                // - Type should no be nested/object
                //   Not supported to filter on
                // - index cannot be NO

                String type = (String) properties.get("type");
                String index = (String) properties.get("index");
                if ((index == null || "not_analyzed".equals(index))
                    &&
                    !("object".equals(type)
                        || "nested".equals(type)))
                {
                    result.add(entry.getKey());
                }
            }
            return result;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error fetching mappings from " + esType.endpoint);
        }
    }
}
