package se.kuseman.payloadbuilder.catalog.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.SortOrder;

/** TupleIterator that polls Kafka and produces TupleVector batches */
class KafkaTupleIterator implements TupleIterator
{
    //@formatter:off
    static final Schema SCHEMA = Schema.of(
            Column.of("key", ResolvedType.of(Type.Any)),
            Column.of("value", ResolvedType.of(Type.Any)),
            Column.of("offset", ResolvedType.of(Type.Long)),
            Column.of("partition", ResolvedType.of(Type.Int)),
            Column.of("timestamp", ResolvedType.of(Type.DateTimeOffset)),
            Column.of("timestampType", ResolvedType.of(Type.String)),
            Column.of("headers", ResolvedType.of(Type.Any)),
            Column.of("topic", ResolvedType.of(Type.String)));
    //@formatter:on

    static final int COL_KEY = 0;
    static final int COL_VALUE = 1;
    static final int COL_OFFSET = 2;
    static final int COL_PARTITION = 3;
    static final int COL_TIMESTAMP = 4;
    static final int COL_TIMESTAMPTYPE = 5;
    static final int COL_HEADERS = 6;
    static final int COL_TOPIC = 7;

    private static final int MAX_EMPTY_POLLS = 3;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<Integer, KafkaSplit> splitByPartition;
    private final Set<Integer> completedPartitions = new HashSet<>();
    private final IRecordDeserializer deserializer;
    private final KafkaNodeData nodeData;
    private final int batchSize;
    private final boolean streaming;
    private final OnError onError;
    private final SortOrder sortOrder;
    private final IExecutionContext context;
    private final Runnable abortListener;
    private final long pollTimeoutMs;

    private TupleVector pendingBatch;
    private List<Object> newestKeys;
    private List<byte[]> newestRawValues;
    private List<Long> newestOffsets;
    private List<Integer> newestPartitions;
    private List<Long> newestTimestamps;
    private List<UTF8String> newestTimestampTypes;
    private List<Object> newestHeaders;
    private List<UTF8String> newestTopics;
    private int newestCursor;
    private boolean newestBufferLoaded;
    private boolean closed;

    //@formatter:off
    KafkaTupleIterator(
            KafkaConsumer<byte[], byte[]> consumer,
            List<KafkaSplit> splits,
            IRecordDeserializer deserializer,
            KafkaNodeData nodeData,
            int batchSize,
            boolean streaming,
            OnError onError,
            SortOrder sortOrder,
            IExecutionContext context,
            Runnable abortListener,
            long pollTimeoutMs)
    //@formatter:on
    {
        this.consumer = consumer;
        this.splitByPartition = new HashMap<>();
        for (KafkaSplit split : splits)
        {
            this.splitByPartition.put(split.partition(), split);
        }
        this.deserializer = deserializer;
        this.nodeData = nodeData;
        this.batchSize = batchSize;
        this.streaming = streaming;
        this.onError = onError;
        this.sortOrder = sortOrder;
        this.context = context;
        this.abortListener = abortListener;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    @Override
    public boolean isBlocking()
    {
        return streaming
                && pendingBatch == null;
    }

    @Override
    public boolean hasNext()
    {
        if (closed)
        {
            return false;
        }

        try
        {
            return pendingBatch != null
                    || fetchNextBatch();
        }
        catch (WakeupException e)
        {
            // Don't propagate exceptions when aborting
            return false;
        }
    }

    @Override
    public TupleVector next()
    {
        if (pendingBatch == null)
        {
            throw new NoSuchElementException();
        }
        TupleVector result = pendingBatch;
        pendingBatch = null;
        return result;
    }

    @Override
    public void close()
    {
        if (!closed)
        {
            closed = true;
            context.getSession()
                    .unregisterAbortListener(abortListener);
            consumer.close(Duration.ofSeconds(5));
        }
    }

    @Override
    public int estimatedBatchCount()
    {
        long total = splitByPartition.values()
                .stream()
                .mapToLong(KafkaSplit::estimatedRecordCount)
                .filter(c -> c >= 0)
                .sum();
        return total > 0 ? (int) Math.ceil((double) total / batchSize)
                : -1;
    }

    private boolean fetchNextBatch()
    {
        if (!streaming
                && sortOrder == SortOrder.NEWEST)
        {
            return fetchNewestBatch();
        }

        if (allSplitsComplete()
                && !streaming)
        {
            return false;
        }

        // Accumulation buffers
        List<Object> keys = new ArrayList<>(batchSize);
        List<byte[]> rawValues = new ArrayList<>(batchSize);
        List<Long> offsets = new ArrayList<>(batchSize);
        List<Integer> partitions = new ArrayList<>(batchSize);
        List<Long> timestamps = new ArrayList<>(batchSize);
        List<UTF8String> timestampTypes = new ArrayList<>(batchSize);
        List<Object> headers = new ArrayList<>(batchSize);
        List<UTF8String> topics = new ArrayList<>(batchSize);

        int accumulated = 0;
        int emptyPollCount = 0;

        while (accumulated < batchSize)
        {
            if (context.getSession()
                    .abortQuery())
            {
                return accumulated > 0;
            }

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            nodeData.pollCount++;

            if (records.isEmpty())
            {
                if (!streaming
                        && allSplitsComplete())
                {
                    break;
                }
                emptyPollCount++;

                // In stream mode: return partial batch if we have data
                // In batch mode: give up after MAX_EMPTY_POLLS
                if (accumulated > 0
                        || (!streaming
                                && emptyPollCount > MAX_EMPTY_POLLS))
                {
                    break;
                }
                continue;
            }
            emptyPollCount = 0;

            for (ConsumerRecord<byte[], byte[]> record : records)
            {
                int partition = record.partition();
                KafkaSplit split = splitByPartition.get(partition);

                // Check if record is past split boundary
                if (split != null
                        && split.isComplete(record.offset()))
                {
                    completedPartitions.add(partition);
                    continue;
                }

                // Deserialize key eagerly (cheap, used in filters)
                Object key;
                try
                {
                    key = deserializer.deserializeKey(record.key());
                }
                catch (Exception e)
                {
                    nodeData.deserializationErrors++;
                    if (onError == OnError.FAIL)
                    {
                        throw new RuntimeException("Key deserialization error at " + partition + ":" + record.offset(), e);
                    }
                    key = null;
                }

                keys.add(key);
                // Store raw value bytes for lazy deserialization
                rawValues.add(record.value());
                offsets.add(record.offset());
                partitions.add(partition);
                timestamps.add(record.timestamp());
                timestampTypes.add(UTF8String.from(record.timestampType().name));
                headers.add(convertHeaders(record.headers()));
                topics.add(UTF8String.from(record.topic()));

                nodeData.recordsPolled++;
                nodeData.bytesRead += estimateRecordSize(record);
                accumulated++;

                // Mark split complete if next offset reaches end
                if (split != null
                        && split.isComplete(record.offset() + 1))
                {
                    completedPartitions.add(partition);
                }

                if (accumulated >= batchSize)
                {
                    break;
                }
            }

            // Pause completed partitions and break early if all done
            pauseCompletedPartitions();

            if (!streaming
                    && allSplitsComplete())
            {
                break;
            }

            if (accumulated >= batchSize)
            {
                break;
            }
        }

        if (accumulated == 0)
        {
            return false;
        }

        pendingBatch = buildTupleVector(keys, rawValues, offsets, partitions, timestamps, timestampTypes, headers, topics);
        return true;
    }

    private boolean fetchNewestBatch()
    {
        if (!newestBufferLoaded)
        {
            loadNewestBuffer();
            newestBufferLoaded = true;
        }

        if (newestOffsets == null
                || newestCursor >= newestOffsets.size())
        {
            return false;
        }

        int toIndex = Math.min(newestCursor + batchSize, newestOffsets.size());
        pendingBatch = buildTupleVector(slice(newestKeys, newestCursor, toIndex), slice(newestRawValues, newestCursor, toIndex), slice(newestOffsets, newestCursor, toIndex),
                slice(newestPartitions, newestCursor, toIndex), slice(newestTimestamps, newestCursor, toIndex), slice(newestTimestampTypes, newestCursor, toIndex),
                slice(newestHeaders, newestCursor, toIndex), slice(newestTopics, newestCursor, toIndex));

        newestCursor = toIndex;
        return true;
    }

    private void loadNewestBuffer()
    {
        List<Object> keys = new ArrayList<>();
        List<byte[]> rawValues = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        List<Integer> partitions = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<UTF8String> timestampTypes = new ArrayList<>();
        List<Object> headers = new ArrayList<>();
        List<UTF8String> topics = new ArrayList<>();

        int emptyPollCount = 0;

        while (true)
        {
            if (context.getSession()
                    .abortQuery())
            {
                break;
            }

            if (allSplitsComplete())
            {
                break;
            }

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            nodeData.pollCount++;

            if (records.isEmpty())
            {
                emptyPollCount++;
                if (emptyPollCount > MAX_EMPTY_POLLS)
                {
                    break;
                }
                continue;
            }

            emptyPollCount = 0;

            for (ConsumerRecord<byte[], byte[]> record : records)
            {
                int partition = record.partition();
                KafkaSplit split = splitByPartition.get(partition);

                if (split != null
                        && split.isComplete(record.offset()))
                {
                    completedPartitions.add(partition);
                    continue;
                }

                Object key;
                try
                {
                    key = deserializer.deserializeKey(record.key());
                }
                catch (Exception e)
                {
                    nodeData.deserializationErrors++;
                    if (onError == OnError.FAIL)
                    {
                        throw new RuntimeException("Key deserialization error at " + partition + ":" + record.offset(), e);
                    }
                    key = null;
                }

                keys.add(key);
                rawValues.add(record.value());
                offsets.add(record.offset());
                partitions.add(partition);
                timestamps.add(record.timestamp());
                timestampTypes.add(UTF8String.from(record.timestampType().name));
                headers.add(convertHeaders(record.headers()));
                topics.add(UTF8String.from(record.topic()));

                nodeData.recordsPolled++;
                nodeData.bytesRead += estimateRecordSize(record);

                if (split != null
                        && split.isComplete(record.offset() + 1))
                {
                    completedPartitions.add(partition);
                }
            }

            pauseCompletedPartitions();
        }

        if (!offsets.isEmpty())
        {
            reorderNewest(keys, rawValues, offsets, partitions, timestamps, timestampTypes, headers, topics);
        }

        newestKeys = keys;
        newestRawValues = rawValues;
        newestOffsets = offsets;
        newestPartitions = partitions;
        newestTimestamps = timestamps;
        newestTimestampTypes = timestampTypes;
        newestHeaders = headers;
        newestTopics = topics;
        newestCursor = 0;
    }

    private static <T> List<T> slice(List<T> values, int fromIndex, int toIndex)
    {
        return new ArrayList<>(values.subList(fromIndex, toIndex));
    }

    //@formatter:off
    private static void reorderNewest(
            List<Object> keys,
            List<byte[]> rawValues,
            List<Long> offsets,
            List<Integer> partitions,
            List<Long> timestamps,
            List<UTF8String> timestampTypes,
            List<Object> headers,
            List<UTF8String> topics)
    //@formatter:on
    {
        int size = offsets.size();
        if (size <= 1)
        {
            return;
        }

        List<Integer> order = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            order.add(i);
        }

        order.sort(Comparator.comparing((Integer i) -> timestamps.get(i), Comparator.reverseOrder())
                .thenComparing(i -> offsets.get(i), Comparator.reverseOrder())
                .thenComparing(i -> partitions.get(i)));

        reorderList(keys, order);
        reorderList(rawValues, order);
        reorderList(offsets, order);
        reorderList(partitions, order);
        reorderList(timestamps, order);
        reorderList(timestampTypes, order);
        reorderList(headers, order);
        reorderList(topics, order);
    }

    private static <T> void reorderList(List<T> values, List<Integer> order)
    {
        List<T> sorted = new ArrayList<>(order.size());
        for (int index : order)
        {
            sorted.add(values.get(index));
        }
        values.clear();
        values.addAll(sorted);
    }

    private boolean allSplitsComplete()
    {
        return completedPartitions.size() >= splitByPartition.size();
    }

    private void pauseCompletedPartitions()
    {
        List<TopicPartition> toPause = new ArrayList<>();
        for (int p : completedPartitions)
        {
            TopicPartition tp = new TopicPartition(splitByPartition.get(p)
                    .topic(), p);
            if (!consumer.paused()
                    .contains(tp))
            {
                toPause.add(tp);
            }
        }
        if (!toPause.isEmpty())
        {
            consumer.pause(toPause);
        }
    }

    //@formatter:off
    private TupleVector buildTupleVector(
            List<Object> keys,
            List<byte[]> rawValues,
            List<Long> offsets,
            List<Integer> partitions,
            List<Long> timestamps,
            List<UTF8String> timestampTypes,
            List<Object> headers,
            List<UTF8String> topics)
    //@formatter:on
    {
        int rowCount = offsets.size();

        // Build lazy value column
        byte[][] rawPayloadsArray = rawValues.toArray(new byte[0][]);
        ValueVector lazyValueVector = new LazyDeserializingValueVector(rawPayloadsArray, deserializer);

        return new ObjectTupleVector(SCHEMA, rowCount, (row, col) -> switch (col)
        {
            case COL_KEY -> keys.get(row);
            case COL_VALUE -> lazyValueVector.getAny(row);
            case COL_OFFSET -> offsets.get(row);
            case COL_PARTITION -> partitions.get(row);
            case COL_TIMESTAMP -> EpochDateTimeOffset.from(timestamps.get(row));
            case COL_TIMESTAMPTYPE -> timestampTypes.get(row);
            case COL_HEADERS -> headers.get(row);
            case COL_TOPIC -> topics.get(row);
            default -> throw new IllegalArgumentException("Invalid column: " + col);
        });
    }

    private static Map<String, Object> convertHeaders(org.apache.kafka.common.header.Headers kafkaHeaders)
    {
        if (kafkaHeaders == null)
        {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        for (Header header : kafkaHeaders)
        {
            result.put(header.key(), header.value() != null ? new String(header.value(), StandardCharsets.UTF_8)
                    : null);
        }
        return result.isEmpty() ? null
                : result;
    }

    private static long estimateRecordSize(ConsumerRecord<byte[], byte[]> record)
    {
        long size = 0;
        if (record.key() != null)
        {
            size += record.key().length;
        }
        if (record.value() != null)
        {
            size += record.value().length;
        }
        return size;
    }
}
