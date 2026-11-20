package se.kuseman.payloadbuilder.catalog.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
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

    /**
     * Number of consecutive empty polls with no partition progress (no completed partition and no fetch position advancing) before a batch-mode scan gives up and fails loudly. A poll returning no
     * records does not by itself mean there is no more data - Kafka can advance the fetch position past a run of transaction control/marker offsets without ever surfacing a record for them - so
     * completion/progress is judged by comparing the fetch position against each split's end offset rather than by counting empty polls alone.
     */
    private static final int MAX_STALLED_POLLS = 60;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<Integer, KafkaSplit> splitByPartition;
    private final Set<Integer> completedPartitions = new HashSet<>();
    private final Map<Integer, Long> lastKnownPositions = new HashMap<>();
    private Iterator<ConsumerRecord<byte[], byte[]>> pendingRecordIterator;
    private final IRecordDeserializer deserializer;
    private final KafkaNodeData nodeData;
    private final int batchSize;
    private final boolean streaming;
    private final OnError onError;
    private final SortOrder sortOrder;
    private final List<KafkaSortColumn> pushedSort;
    private final KafkaProjection projection;
    private final UTF8String topic;
    private final IExecutionContext context;
    private final Runnable abortListener;
    private final long pollTimeoutMs;

    private TupleVector pendingBatch;
    private KafkaRecordBuffer newestBuffer;
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
            List<KafkaSortColumn> pushedSort,
            KafkaProjection projection,
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
        // All splits belong to the same topic, share one instance instead of one per record
        this.topic = splits.isEmpty() ? null
                : UTF8String.from(splits.get(0)
                        .topic());
        this.projection = projection;
        this.deserializer = deserializer;
        this.nodeData = nodeData;
        this.batchSize = batchSize;
        this.streaming = streaming;
        this.onError = onError;
        this.sortOrder = sortOrder;
        this.pushedSort = pushedSort;
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

    /**
     * Upper bound of the rows this scan produces (offset span of all splits - compaction, transaction markers and on_error='skip' can make the actual count lower). Without it the engine treats the
     * scan as unknown size, and a hash join with a large outer side then hashes this side - ie. buffers the whole topic in memory. Unknown (-1) in stream mode or when any split is unbounded.
     */
    @Override
    public int estimatedRowCount()
    {
        if (streaming)
        {
            return -1;
        }
        long total = 0;
        for (KafkaSplit split : splitByPartition.values())
        {
            long count = split.estimatedRecordCount();
            if (count < 0)
            {
                return -1;
            }
            total += count;
        }
        return (int) Math.min(total, Integer.MAX_VALUE);
    }

    private boolean fetchNextBatch()
    {
        // A pushed-down ORDER BY (pushedSort) requires the exact requested order, and a WITH-clause
        // sort_order='newest' requires the default newest-first order when no ORDER BY was pushed - both need
        // every matching record buffered up front before it can be handed out in the right order.
        if (!streaming
                && (pushedSort != null
                        || sortOrder == SortOrder.NEWEST))
        {
            return fetchNewestBatch();
        }

        if (allSplitsComplete()
                && !streaming)
        {
            return false;
        }

        KafkaRecordBuffer buffer = new KafkaRecordBuffer(batchSize, projection);

        int accumulated = 0;
        int stalledPollCount = 0;

        while (true)
        {
            if (context.getSession()
                    .abortQuery())
            {
                return accumulated > 0;
            }

            // A single poll() can return records spanning multiple partitions. If a batch fills up mid-way
            // through that response, the leftover records must carry over to the next call instead of being
            // discarded - Kafka won't hand them back, since the fetch position has already moved past them.
            if (pendingRecordIterator == null
                    || !pendingRecordIterator.hasNext())
            {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                nodeData.pollCount++;

                if (records.isEmpty())
                {
                    boolean progressed = !streaming
                            && refreshProgress();

                    if (!streaming
                            && allSplitsComplete())
                    {
                        break;
                    }

                    // In stream mode: return partial batch if we have data
                    if (accumulated > 0)
                    {
                        break;
                    }

                    if (!streaming)
                    {
                        stalledPollCount = progressed ? 0
                                : stalledPollCount + 1;
                        failIfStalled(stalledPollCount);
                    }
                    continue;
                }

                stalledPollCount = 0;
                pendingRecordIterator = records.iterator();
            }

            while (pendingRecordIterator.hasNext()
                    && accumulated < batchSize)
            {
                ConsumerRecord<byte[], byte[]> record = pendingRecordIterator.next();
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

                // SKIP must drop malformed rows entirely, so unlike NULL/FAIL it can't rely on the value being
                // deserialized lazily - it has to be validated eagerly, before the row is added to the batch.
                if (onError == OnError.SKIP
                        && isValueMalformed(record.value()))
                {
                    nodeData.deserializationErrors++;
                    if (split != null
                            && split.isComplete(record.offset() + 1))
                    {
                        completedPartitions.add(partition);
                    }
                    continue;
                }

                buffer.add(record, key);

                nodeData.recordsPolled++;
                nodeData.bytesRead += estimateRecordSize(record);
                accumulated++;

                // Mark split complete if next offset reaches end
                if (split != null
                        && split.isComplete(record.offset() + 1))
                {
                    completedPartitions.add(partition);
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

        pendingBatch = buffer.toTupleVector(topic, deserializer, onError, nodeData);
        return true;
    }

    private boolean fetchNewestBatch()
    {
        if (!newestBufferLoaded)
        {
            loadNewestBuffer();
            newestBufferLoaded = true;
        }

        if (newestBuffer == null
                || newestCursor >= newestBuffer.size())
        {
            // Release the buffered records
            newestBuffer = null;
            return false;
        }

        int toIndex = Math.min(newestCursor + batchSize, newestBuffer.size());
        pendingBatch = newestBuffer.slice(newestCursor, toIndex)
                .toTupleVector(topic, deserializer, onError, nodeData);

        newestCursor = toIndex;
        return true;
    }

    private void loadNewestBuffer()
    {
        KafkaRecordBuffer buffer = new KafkaRecordBuffer(batchSize, projection);

        int stalledPollCount = 0;

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
                boolean progressed = refreshProgress();
                if (allSplitsComplete())
                {
                    break;
                }

                stalledPollCount = progressed ? 0
                        : stalledPollCount + 1;
                failIfStalled(stalledPollCount);
                continue;
            }

            stalledPollCount = 0;

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

                if (onError == OnError.SKIP
                        && isValueMalformed(record.value()))
                {
                    nodeData.deserializationErrors++;
                    if (split != null
                            && split.isComplete(record.offset() + 1))
                    {
                        completedPartitions.add(partition);
                    }
                    continue;
                }

                buffer.add(record, key);

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

        if (buffer.size() > 1)
        {
            reorderNewest(buffer, pushedSort);
        }

        newestBuffer = buffer;
        newestCursor = 0;
    }

    private static void reorderNewest(KafkaRecordBuffer buffer, List<KafkaSortColumn> pushedSort)
    {
        int size = buffer.size();
        Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++)
        {
            order[i] = i;
        }

        Arrays.sort(order, buildComparator(pushedSort, buffer).thenComparingInt(buffer::partition));

        int[] rows = new int[size];
        for (int i = 0; i < size; i++)
        {
            rows[i] = order[i];
        }
        buffer.reorder(rows);
    }

    /**
     * Build the comparator used to physically order buffered rows. A pushed-down ORDER BY (offset/timestamp, always DESC) is honored in exactly the requested column sequence; with no pushed sort -
     * ie. only the WITH-clause sort_order='newest' was requested - the default is timestamp DESC then offset DESC. Either way partition ASC is applied by the caller as a final tie-breaker.
     */
    private static Comparator<Integer> buildComparator(List<KafkaSortColumn> pushedSort, KafkaRecordBuffer buffer)
    {
        List<KafkaSortColumn> columns = (pushedSort != null
                && !pushedSort.isEmpty()) ? pushedSort
                        : List.of(KafkaSortColumn.TIMESTAMP, KafkaSortColumn.OFFSET);

        Comparator<Integer> comparator = null;
        for (KafkaSortColumn column : columns)
        {
            Comparator<Integer> next = column == KafkaSortColumn.OFFSET ? (a, b) -> Long.compare(buffer.offset(b), buffer.offset(a))
                    : (a, b) -> Long.compare(buffer.timestamp(b), buffer.timestamp(a));
            comparator = comparator == null ? next
                    : comparator.thenComparing(next);
        }
        return comparator;
    }

    /** Eagerly validate a raw value payload for on_error='skip', without keeping the deserialized result around (the value column stays lazily deserialized on actual access). */
    private boolean isValueMalformed(byte[] value)
    {
        if (value == null)
        {
            return false;
        }
        try
        {
            deserializer.deserializeValue(value);
            return false;
        }
        catch (Exception e)
        {
            return true;
        }
    }

    private boolean allSplitsComplete()
    {
        return completedPartitions.size() >= splitByPartition.size();
    }

    /**
     * Refreshes completion status of not-yet-complete partitions by comparing their current fetch position against the split's end offset, and records position movement even when no record was seen
     * for a partition. Returns true if any partition became complete or its position advanced since the previous call - i.e. the scan made real progress even though the last poll returned no records.
     */
    private boolean refreshProgress()
    {
        boolean progressed = false;
        for (Map.Entry<Integer, KafkaSplit> entry : splitByPartition.entrySet())
        {
            int partition = entry.getKey();
            if (completedPartitions.contains(partition))
            {
                continue;
            }

            KafkaSplit split = entry.getValue();
            long position = consumer.position(new TopicPartition(split.topic(), partition));
            if (split.isComplete(position))
            {
                completedPartitions.add(partition);
                progressed = true;
                continue;
            }

            Long previous = lastKnownPositions.put(partition, position);
            if (previous == null
                    || position > previous)
            {
                progressed = true;
            }
        }
        return progressed;
    }

    /** Throws if the scan has made no progress for too many consecutive empty polls, rather than silently returning incomplete results. */
    private void failIfStalled(int stalledPollCount)
    {
        if (stalledPollCount <= MAX_STALLED_POLLS)
        {
            return;
        }

        int incomplete = splitByPartition.size() - completedPartitions.size();
        String topic = splitByPartition.values()
                .iterator()
                .next()
                .topic();
        throw new IllegalStateException("Kafka scan of topic '" + topic
                                        + "' stalled: no progress after "
                                        + stalledPollCount
                                        + " consecutive empty polls (~"
                                        + (stalledPollCount * pollTimeoutMs)
                                        + "ms) while "
                                        + incomplete
                                        + " partition(s) have not reached their target offset. This may indicate a broker or network issue.");
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
