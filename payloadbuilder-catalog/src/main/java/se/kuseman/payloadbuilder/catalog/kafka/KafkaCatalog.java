package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Catalog for querying Apache Kafka */
public class KafkaCatalog extends Catalog
{
    static final String NAME = "Kafka";

    // Catalog property keys
    static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
    static final String SCHEMA_REGISTRY_URL = "schema_registry_url";
    static final String SECURITY_PROTOCOL = "security_protocol";
    static final String SASL_MECHANISM = "sasl_mechanism";
    static final String SASL_JAAS_CONFIG = "sasl_jaas_config";
    static final String TOPIC = "topic";
    private static final QualifiedName OFFSET = QualifiedName.of("offset");
    private static final QualifiedName TIMESTAMP = QualifiedName.of("timestamp");

    /** Construct a new Kafka catalog */
    public KafkaCatalog()
    {
        super(NAME);
    }

    @Override
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        // Fully schema-less: all columns are Type.Any, discovered at runtime
        return new TableSchema(Schema.EMPTY);
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        String entityType = table.getFirst();

        // Resolve entity name: use second part if present, otherwise fall back to catalog property
        String entityName;
        if (table.size() >= 2)
        {
            entityName = table.getLast();
        }
        else
        {
            // Single-part name like kafka#topic -> resolve topic name from catalog property
            entityName = resolveFromCatalogProperty(session, catalogAlias, entityType);
        }

        KafkaPredicateAnalysis predicateAnalysis = KafkaPredicateAnalysis.analyze(data.getPredicates());
        KafkaOptions.SortOrder sortOrder = getSortOrder(data.getSortItems());

        return switch (entityType.toLowerCase())
        {
            case "topic" -> new KafkaDatasource(data.getNodeId(), catalogAlias, entityName, predicateAnalysis, data.getOptions(), sortOrder);
            case "metadata" -> new KafkaMetadataDatasource(catalogAlias, entityName);
            case "consumer_group" -> new KafkaConsumerGroupDatasource(catalogAlias, entityName);
            default -> throw new IllegalArgumentException("Unknown Kafka entity type: '" + entityType + "'. Supported: topic, metadata, consumer_group");
        };
    }

    private static KafkaOptions.SortOrder getSortOrder(List<? extends ISortItem> sortItems)
    {
        if (sortItems.isEmpty())
        {
            return null;
        }

        KafkaOptions.SortOrder sortOrder = null;
        boolean hasTimestampOrOffset = false;

        for (ISortItem sortItem : sortItems)
        {
            QualifiedName column = sortItem.getExpression()
                    .getQualifiedColumn();
            if (column == null)
            {
                return null;
            }

            if (!column.equals(OFFSET)
                    && !column.equals(TIMESTAMP))
            {
                return null;
            }

            hasTimestampOrOffset = true;

            if (sortItem.getOrder() != ISortItem.Order.DESC)
            {
                return null;
            }

            KafkaOptions.SortOrder itemOrder = KafkaOptions.SortOrder.NEWEST;
            if (sortOrder == null)
            {
                sortOrder = itemOrder;
            }
            else if (sortOrder != itemOrder)
            {
                return null;
            }
        }

        if (!hasTimestampOrOffset)
        {
            return null;
        }

        sortItems.clear();
        return sortOrder;
    }

    private static String resolveFromCatalogProperty(IQuerySession session, String catalogAlias, String entityType)
    {
        if (!"topic".equalsIgnoreCase(entityType))
        {
            throw new IllegalArgumentException("Entity type '" + entityType + "' requires a name. Use: " + catalogAlias + "#" + entityType + ".<name>");
        }
        ValueVector value = session.getCatalogProperty(catalogAlias, TOPIC);
        if (value == null
                || value.isNull(0))
        {
            throw new IllegalArgumentException("No topic name specified. Either use " + catalogAlias + "#topic.<name> or set catalog property '" + TOPIC + "'");
        }
        return value.valueAsString(0);
    }

    @Override
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        if (table.size() == 1)
        {
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return TableSchema.EMPTY;
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_FUNCTIONS_SCHEMA);
            }
        }
        throw new RuntimeException(table + " is not supported");
    }

    @Override
    public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        String type = table.getLast();
        if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return context -> TupleIterator.singleton(getFunctionsTupleVector(SYS_FUNCTIONS_SCHEMA));
        }
        throw new RuntimeException(table + " is not supported");
    }
}
