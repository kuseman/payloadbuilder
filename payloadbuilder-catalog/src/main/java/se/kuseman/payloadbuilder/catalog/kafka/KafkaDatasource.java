package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** IDatasource for Kafka topic data */
class KafkaDatasource implements IDatasource
{
    private final int nodeId;
    private final String catalogAlias;
    private final String topic;
    private final KafkaPredicateAnalysis predicateAnalysis;
    private final List<Option> options;
    private final List<KafkaSortColumn> pushedSort;
    private final boolean hasResidualPredicates;

    KafkaDatasource(int nodeId, String catalogAlias, String topic, KafkaPredicateAnalysis predicateAnalysis, List<Option> options, List<KafkaSortColumn> pushedSort, boolean hasResidualPredicates)
    {
        this.nodeId = nodeId;
        this.catalogAlias = catalogAlias;
        this.topic = topic;
        this.predicateAnalysis = predicateAnalysis;
        this.options = options;
        this.pushedSort = pushedSort;
        this.hasResidualPredicates = hasResidualPredicates;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        KafkaNodeData nodeData = context.getStatementContext()
                .getOrCreateNodeData(nodeId, KafkaNodeData::new);

        // Parse WITH options (includes batch_size to avoid IExecutionContext default method issues)
        KafkaOptions kafkaOptions = KafkaOptions.from(context, options);

        // A pushed-down ORDER BY can only be honored by buffering every matching record up front and sorting it
        // (see KafkaTupleIterator.fetchNewestBatch), which is impossible against an unbounded stream - fail fast
        // here rather than silently returning stream-mode data in poll order while claiming the order is satisfied.
        if (pushedSort != null
                && kafkaOptions.mode() == KafkaOptions.ExecutionMode.STREAM)
        {
            throw new IllegalArgumentException("ORDER BY on offset/timestamp is not supported in stream mode");
        }

        // Create viewer-mode consumer
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerFactory.createConsumer(context.getSession(), catalogAlias);

        try
        {
            // Resolve splits (applies predicate-based partition/offset narrowing)
            List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, topic, kafkaOptions, predicateAnalysis, context, hasResidualPredicates);

            if (splits.isEmpty())
            {
                consumer.close();
                return TupleIterator.EMPTY;
            }

            // Assign and seek all partitions
            List<TopicPartition> topicPartitions = splits.stream()
                    .map(s -> new TopicPartition(s.topic(), s.partition()))
                    .toList();
            consumer.assign(topicPartitions);
            for (KafkaSplit split : splits)
            {
                consumer.seek(new TopicPartition(split.topic(), split.partition()), split.startOffset());
            }

            // Create deserializer
            IRecordDeserializer deserializer = RecordDeserializerFactory.create(kafkaOptions.format(), context.getSession(), catalogAlias);

            // Register abort listener
            Runnable abortListener = consumer::wakeup;
            context.getSession()
                    .registerAbortListener(abortListener);

            boolean streaming = kafkaOptions.mode() == KafkaOptions.ExecutionMode.STREAM;

            return new KafkaTupleIterator(consumer, splits, deserializer, nodeData, kafkaOptions.batchSize(), streaming, kafkaOptions.onError(), kafkaOptions.sortOrder(), pushedSort, context,
                    abortListener, kafkaOptions.pollTimeoutMs());
        }
        catch (Exception e)
        {
            consumer.close();
            throw e;
        }
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(CATALOG, KafkaCatalog.NAME);
        props.put("Topic", topic);

        KafkaNodeData data = context.getStatementContext()
                .getNodeData(nodeId);
        if (data != null)
        {
            props.put("Records polled", data.recordsPolled);
            props.put("Bytes read", data.bytesRead);
            props.put("Poll count", data.pollCount);
            if (data.deserializationErrors > 0)
            {
                props.put("Deserialization errors", data.deserializationErrors);
            }
        }
        return props;
    }
}
