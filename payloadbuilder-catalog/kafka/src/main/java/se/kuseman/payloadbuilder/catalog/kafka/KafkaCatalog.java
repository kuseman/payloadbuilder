package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.ArrayList;
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
    public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
    public static final String SCHEMA_REGISTRY_URL = "schema_registry_url";
    public static final String SECURITY_PROTOCOL = "security_protocol";
    public static final String SASL_MECHANISM = "sasl_mechanism";
    public static final String SASL_JAAS_CONFIG = "sasl_jaas_config";
    public static final String TOPIC = "topic";
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
        // Predicates left after analysis are evaluated by the engine after data leaves the data source. If any
        // remain, a "newest" tail-window narrowing would silently drop matching records outside that window,
        // so the data source must scan the full requested range instead.
        boolean hasResidualPredicates = !data.getPredicates()
                .isEmpty();
        // The exact ORDER BY consumed (if any), kept separate from the WITH-clause sort_order/tail_count feature
        // in KafkaOptions - the two are unrelated: a pushed ORDER BY only promises the returned rows will be in
        // that order, it must not narrow which rows are returned.
        List<KafkaSortColumn> pushedSort = getPushedSort(data.getSortItems());

        return switch (entityType.toLowerCase())
        {
            case "topic" -> new KafkaDatasource(data.getNodeId(), catalogAlias, entityName, predicateAnalysis, data.getOptions(), pushedSort, hasResidualPredicates);
            case "metadata" -> new KafkaMetadataDatasource(catalogAlias, entityName);
            case "consumer_group" -> new KafkaConsumerGroupDatasource(catalogAlias, entityName);
            default -> throw new IllegalArgumentException("Unknown Kafka entity type: '" + entityType + "'. Supported: topic, metadata, consumer_group");
        };
    }

    /**
     * Determine whether the requested sort items can be satisfied natively (a sequence of {@code offset}/{@code timestamp} columns, all descending) and, if so, consume them and return the exact
     * column sequence to sort by. Returns {@code null} (consuming nothing) otherwise - all or none of the sort items are ever consumed.
     */
    private static List<KafkaSortColumn> getPushedSort(List<? extends ISortItem> sortItems)
    {
        if (sortItems.isEmpty())
        {
            return null;
        }

        List<KafkaSortColumn> columns = new ArrayList<>(sortItems.size());

        for (ISortItem sortItem : sortItems)
        {
            QualifiedName column = sortItem.getExpression()
                    .getQualifiedColumn();
            if (column == null
                    || sortItem.getOrder() != ISortItem.Order.DESC)
            {
                return null;
            }

            if (column.equals(OFFSET))
            {
                columns.add(KafkaSortColumn.OFFSET);
            }
            else if (column.equals(TIMESTAMP))
            {
                columns.add(KafkaSortColumn.TIMESTAMP);
            }
            else
            {
                return null;
            }
        }

        sortItems.clear();
        return columns;
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
