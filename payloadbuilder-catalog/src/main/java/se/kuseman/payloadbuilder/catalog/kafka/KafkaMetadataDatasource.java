package se.kuseman.payloadbuilder.catalog.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** IDatasource for Kafka metadata queries (topics, partitions) */
class KafkaMetadataDatasource implements IDatasource
{
    //@formatter:off
    private static final Schema TOPICS_SCHEMA = Schema.of(
            Column.of("name", ResolvedType.of(Type.String)),
            Column.of("partition_count", ResolvedType.of(Type.Int)),
            Column.of("replication_factor", ResolvedType.of(Type.Int)),
            Column.of("is_internal", ResolvedType.of(Type.Boolean)));

    private static final Schema CONSUMER_SCHEMA = Schema.of(
            Column.of("groupid", ResolvedType.of(Type.String)),
            Column.of("simple_group", ResolvedType.of(Type.Boolean)),
            Column.of("state", ResolvedType.of(Type.String)));

    private static final Schema PARTITIONS_SCHEMA = Schema.of(
            Column.of("topic", ResolvedType.of(Type.String)),
            Column.of("partition", ResolvedType.of(Type.Int)),
            Column.of("leader", ResolvedType.of(Type.Int)),
            Column.of("replicas", ResolvedType.of(Type.String)),
            Column.of("isr", ResolvedType.of(Type.String)),
            Column.of("begin_offset", ResolvedType.of(Type.Long)),
            Column.of("end_offset", ResolvedType.of(Type.Long)));
    //@formatter:on

    private final String catalogAlias;
    private final String metadataType;

    KafkaMetadataDatasource(String catalogAlias, String metadataType)
    {
        this.catalogAlias = catalogAlias;
        this.metadataType = metadataType;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        return switch (metadataType.toLowerCase())
        {
            case "topics" -> executeTopics(context);
            case "partitions" -> executePartitions(context);
            case "consumers" -> executeConsumerGroups(context);
            default -> throw new IllegalArgumentException("Unknown metadata type: '" + metadataType + "'. Supported: topics, partitions");
        };
    }

    private TupleIterator executeTopics(IExecutionContext context)
    {
        try (AdminClient admin = KafkaAdminFactory.createAdmin(context.getSession(), catalogAlias))
        {
            Collection<TopicListing> listings = admin.listTopics()
                    .listings()
                    .get();

            Set<String> topicNames = admin.listTopics()
                    .names()
                    .get();
            Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames)
                    .allTopicNames()
                    .get();

            List<TopicListing> topicList = new ArrayList<>(listings);

            return TupleIterator.singleton(new ObjectTupleVector(TOPICS_SCHEMA, topicList.size(), (row, col) ->
            {
                TopicListing listing = topicList.get(row);
                TopicDescription desc = descriptions.get(listing.name());
                // CSOFF
                return switch (col)
                // CSON
                {
                    case 0 -> UTF8String.from(listing.name());
                    case 1 -> desc != null ? desc.partitions()
                            .size()
                            : 0;
                    case 2 -> desc != null
                            && !desc.partitions()
                                    .isEmpty() ? desc.partitions()
                                            .get(0)
                                            .replicas()
                                            .size()
                                            : 0;
                    case 3 -> listing.isInternal();
                    default -> throw new IllegalArgumentException("Invalid column: " + col);
                };
            }));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread()
                    .interrupt();
            throw new RuntimeException("Interrupted while fetching topic metadata", e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Failed to fetch topic metadata", e.getCause());
        }
    }

    private TupleIterator executeConsumerGroups(IExecutionContext context)
    {
        try (AdminClient admin = KafkaAdminFactory.createAdmin(context.getSession(), catalogAlias))
        {
            List<ConsumerGroupListing> listings = new ArrayList<>(admin.listConsumerGroups()
                    .all()
                    .get());

            return TupleIterator.singleton(new ObjectTupleVector(CONSUMER_SCHEMA, listings.size(), (row, col) ->
            {
                ConsumerGroupListing group = listings.get(row);
                // CSOFF
                return switch (col)
                // CSON
                {
                    case 0 -> UTF8String.from(group.groupId());
                    case 1 -> group.isSimpleConsumerGroup();
                    case 2 -> group.state()
                            .map(UTF8String::from)
                            .orElse(null);
                    default -> throw new IllegalArgumentException("Invalid column: " + col);
                };
            }));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread()
                    .interrupt();
            throw new RuntimeException("Interrupted while fetching group metadata", e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Failed to fetch groups metadata", e.getCause());
        }
    }

    private TupleIterator executePartitions(IExecutionContext context)
    {
        try (AdminClient admin = KafkaAdminFactory.createAdmin(context.getSession(), catalogAlias))
        {
            Set<String> topicNames = admin.listTopics()
                    .names()
                    .get();
            Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames)
                    .allTopicNames()
                    .get();

            // Flatten all partitions across all topics
            List<PartitionRow> rows = new ArrayList<>();
            for (TopicDescription desc : descriptions.values())
            {
                for (TopicPartitionInfo pi : desc.partitions())
                {
                    rows.add(new PartitionRow(desc.name(), pi));
                }
            }

            // Get begin/end offsets using a consumer
            KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerFactory.createConsumer(context.getSession(), catalogAlias);
            try
            {
                List<TopicPartition> tps = rows.stream()
                        .map(r -> new TopicPartition(r.topic, r.info.partition()))
                        .toList();
                Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps, Duration.ofSeconds(10));
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps, Duration.ofSeconds(10));

                return TupleIterator.singleton(new ObjectTupleVector(PARTITIONS_SCHEMA, rows.size(), (row, col) ->
                {
                    PartitionRow pr = rows.get(row);
                    TopicPartition tp = new TopicPartition(pr.topic, pr.info.partition());
                    // CSOFF
                    return switch (col)
                    // CSON
                    {
                        case 0 -> UTF8String.from(pr.topic);
                        case 1 -> pr.info.partition();
                        case 2 -> pr.info.leader() != null ? pr.info.leader()
                                .id()
                                : -1;
                        case 3 -> UTF8String.from(pr.info.replicas()
                                .toString());
                        case 4 -> UTF8String.from(pr.info.isr()
                                .toString());
                        case 5 -> beginOffsets.getOrDefault(tp, 0L);
                        case 6 -> endOffsets.getOrDefault(tp, 0L);
                        default -> throw new IllegalArgumentException("Invalid column: " + col);
                    };
                }));
            }
            finally
            {
                consumer.close(Duration.ofSeconds(5));
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread()
                    .interrupt();
            throw new RuntimeException("Interrupted while fetching partition metadata", e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Failed to fetch partition metadata", e.getCause());
        }
    }

    private record PartitionRow(String topic, TopicPartitionInfo info)
    {
    }
}
