package se.kuseman.payloadbuilder.catalog.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** IDatasource for Kafka consumer group offset queries */
class KafkaConsumerGroupDatasource implements IDatasource
{
    //@formatter:off
    private static final Schema SCHEMA = Schema.of(
            Column.of("topic", ResolvedType.of(Type.String)),
            Column.of("partition", ResolvedType.of(Type.Int)),
            Column.of("current_offset", ResolvedType.of(Type.Long)),
            Column.of("end_offset", ResolvedType.of(Type.Long)),
            Column.of("lag", ResolvedType.of(Type.Long)));
    //@formatter:on

    private final String catalogAlias;
    private final String groupName;

    KafkaConsumerGroupDatasource(String catalogAlias, String groupName)
    {
        this.catalogAlias = catalogAlias;
        this.groupName = groupName;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        try (AdminClient admin = KafkaAdminFactory.createAdmin(context.getSession(), catalogAlias))
        {
            // Get committed offsets for the consumer group
            ListConsumerGroupOffsetsResult offsetsResult = admin.listConsumerGroupOffsets(groupName);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = offsetsResult.partitionsToOffsetAndMetadata()
                    .get();

            if (committedOffsets.isEmpty())
            {
                return TupleIterator.EMPTY;
            }

            // Get end offsets for the same partitions
            KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerFactory.createConsumer(context.getSession(), catalogAlias);
            try
            {
                List<TopicPartition> tps = new ArrayList<>(committedOffsets.keySet());
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps, Duration.ofSeconds(10));

                // Build rows
                List<TopicPartition> sortedTps = new ArrayList<>(committedOffsets.keySet());
                sortedTps.sort((a, b) ->
                {
                    int cmp = a.topic()
                            .compareTo(b.topic());
                    return cmp != 0 ? cmp
                            : Integer.compare(a.partition(), b.partition());
                });

                return TupleIterator.singleton(new ObjectTupleVector(SCHEMA, sortedTps.size(), (row, col) ->
                {
                    TopicPartition tp = sortedTps.get(row);
                    long currentOffset = committedOffsets.get(tp)
                            .offset();
                    long endOffset = endOffsets.getOrDefault(tp, 0L);
                    long lag = Math.max(0, endOffset - currentOffset);

                    // CSOFF
                    return switch (col)
                    // CSON
                    {
                        case 0 -> UTF8String.from(tp.topic());
                        case 1 -> tp.partition();
                        case 2 -> currentOffset;
                        case 3 -> endOffset;
                        case 4 -> lag;
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
            throw new RuntimeException("Interrupted while fetching consumer group offsets", e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Failed to fetch consumer group offsets for '" + groupName + "'", e.getCause());
        }
    }
}
