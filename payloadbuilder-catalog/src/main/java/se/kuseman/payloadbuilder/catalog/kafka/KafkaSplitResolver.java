package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Resolves offset bounds into a list of KafkaSplits */
class KafkaSplitResolver
{
    private KafkaSplitResolver()
    {
    }

    /**
     * Resolve splits for a topic based on options and predicate analysis.
     *
     * @param consumer Consumer to use for offset resolution (must not be subscribed)
     * @param topic Topic name
     * @param options Parsed WITH clause options
     * @param predicateAnalysis Extracted predicates for split narrowing (may be null)
     * @param context Execution context for evaluating predicate expressions (may be null if predicateAnalysis is null)
     * @return List of bounded KafkaSplits, one per partition. Empty splits are filtered out.
     */
    static List<KafkaSplit> resolve(KafkaConsumer<byte[], byte[]> consumer, String topic, KafkaOptions options, KafkaPredicateAnalysis predicateAnalysis, IExecutionContext context)
    {
        requireNonNull(consumer, "consumer");
        requireNonNull(topic, "topic");
        requireNonNull(options, "options");

        // Discover partitions
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
        if (partitionInfos == null
                || partitionInfos.isEmpty())
        {
            return List.of();
        }

        List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .toList();

        // Apply partition filter from predicate analysis
        if (predicateAnalysis != null
                && predicateAnalysis.partitionFilter != null
                && context != null)
        {
            Set<Integer> targetPartitions = new HashSet<>();
            for (IExpression expr : predicateAnalysis.partitionFilter)
            {
                ValueVector val = expr.eval(context);
                if (val != null
                        && !val.isNull(0))
                {
                    targetPartitions.add(val.getInt(0));
                }
            }
            if (!targetPartitions.isEmpty())
            {
                topicPartitions = topicPartitions.stream()
                        .filter(tp -> targetPartitions.contains(tp.partition()))
                        .toList();
                if (topicPartitions.isEmpty())
                {
                    return List.of();
                }
            }
        }

        // Resolve start/end offsets from WITH options (wrap in mutable maps for predicate narrowing)
        Map<TopicPartition, Long> startOffsets = new HashMap<>(resolveOffsets(consumer, topicPartitions, options.start(), true));
        Map<TopicPartition, Long> endOffsets = new HashMap<>(resolveEndOffsets(consumer, topicPartitions, options));

        if (options.mode() == KafkaOptions.ExecutionMode.BATCH
                && options.sortOrder() == KafkaOptions.SortOrder.NEWEST)
        {
            Map<TopicPartition, Long> tailStarts = resolveTailStartOffsets(consumer, topicPartitions, options.tailCount());
            for (TopicPartition tp : topicPartitions)
            {
                Long tailStart = tailStarts.get(tp);
                if (tailStart != null)
                {
                    startOffsets.put(tp, tailStart);
                }
            }
        }

        // Apply offset and timestamp narrowing from predicate analysis
        if (predicateAnalysis != null
                && context != null)
        {
            applyOffsetPredicates(consumer, startOffsets, endOffsets, topicPartitions, predicateAnalysis, context);
        }

        // Build splits, filtering empty ones
        List<KafkaSplit> splits = new ArrayList<>();
        for (TopicPartition tp : topicPartitions)
        {
            long start = startOffsets.getOrDefault(tp, 0L);
            long end = endOffsets.getOrDefault(tp, Long.MAX_VALUE);

            if (end != Long.MAX_VALUE
                    && start >= end)
            {
                continue;
            }

            splits.add(new KafkaSplit(topic, tp.partition(), start, end));
        }

        return splits;
    }

    /** Overload without predicate analysis */
    static List<KafkaSplit> resolve(KafkaConsumer<byte[], byte[]> consumer, String topic, KafkaOptions options)
    {
        return resolve(consumer, topic, options, null, null);
    }

    private static void applyOffsetPredicates(KafkaConsumer<byte[], byte[]> consumer, Map<TopicPartition, Long> startOffsets, Map<TopicPartition, Long> endOffsets,
            List<TopicPartition> topicPartitions, KafkaPredicateAnalysis analysis, IExecutionContext context)
    {
        // Offset lower bound: offset >= N (inclusive) or offset > N (exclusive, +1)
        if (analysis.offsetLower != null)
        {
            long value = evalLong(analysis.offsetLower.expression(), context);
            long startOffset = analysis.offsetLower.inclusive() ? value
                    : value + 1;
            for (TopicPartition tp : topicPartitions)
            {
                startOffsets.merge(tp, startOffset, Math::max);
            }
        }

        // Offset upper bound: offset < N (exclusive) or offset <= N (inclusive, +1)
        if (analysis.offsetUpper != null)
        {
            long value = evalLong(analysis.offsetUpper.expression(), context);
            long endOffset = analysis.offsetUpper.inclusive() ? value + 1
                    : value;
            for (TopicPartition tp : topicPartitions)
            {
                endOffsets.merge(tp, endOffset, Math::min);
            }
        }

        // Timestamp lower bound: use offsetsForTimes to find start offset
        // offsetsForTimes returns the earliest offset with timestamp >= given timestamp
        // For >= T: use offsetsForTimes(T)
        // For > T: use offsetsForTimes(T + 1)
        if (analysis.timestampLower != null)
        {
            long value = evalLong(analysis.timestampLower.expression(), context);
            long searchTs = analysis.timestampLower.inclusive() ? value
                    : value + 1;
            Map<TopicPartition, Long> tsOffsets = resolveTimestampOffsets(consumer, topicPartitions, searchTs);
            for (TopicPartition tp : topicPartitions)
            {
                Long tsOffset = tsOffsets.get(tp);
                if (tsOffset != null)
                {
                    startOffsets.merge(tp, tsOffset, Math::max);
                }
            }
        }

        // Timestamp upper bound: use offsetsForTimes to find end offset
        // For < T: use offsetsForTimes(T) as end (exclusive)
        // For <= T: use offsetsForTimes(T + 1) as end (exclusive)
        if (analysis.timestampUpper != null)
        {
            long value = evalLong(analysis.timestampUpper.expression(), context);
            long searchTs = analysis.timestampUpper.inclusive() ? value + 1
                    : value;
            Map<TopicPartition, Long> tsOffsets = resolveTimestampOffsets(consumer, topicPartitions, searchTs);
            for (TopicPartition tp : topicPartitions)
            {
                Long tsOffset = tsOffsets.get(tp);
                if (tsOffset != null)
                {
                    endOffsets.merge(tp, tsOffset, Math::min);
                }
            }
        }
    }

    private static Map<TopicPartition, Long> resolveTimestampOffsets(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions, long timestampMs)
    {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition tp : topicPartitions)
        {
            timestampsToSearch.put(tp, timestampMs);
        }
        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestampsToSearch);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition tp : topicPartitions)
        {
            OffsetAndTimestamp oat = result.get(tp);
            if (oat != null)
            {
                offsets.put(tp, oat.offset());
            }
        }
        return offsets;
    }

    private static long evalLong(IExpression expression, IExecutionContext context)
    {
        ValueVector val = expression.eval(context);
        return val.getLong(0);
    }

    private static Map<TopicPartition, Long> resolveEndOffsets(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions, KafkaOptions options)
    {
        if (options.mode() == KafkaOptions.ExecutionMode.STREAM)
        {
            Map<TopicPartition, Long> result = new HashMap<>();
            for (TopicPartition tp : topicPartitions)
            {
                result.put(tp, Long.MAX_VALUE);
            }
            return result;
        }
        return resolveOffsets(consumer, topicPartitions, options.end(), false);
    }

    private static Map<TopicPartition, Long> resolveTailStartOffsets(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions, int tailCount)
    {
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> latestOffsets = consumer.endOffsets(topicPartitions);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : topicPartitions)
        {
            long beginning = beginningOffsets.getOrDefault(tp, 0L);
            long latest = latestOffsets.getOrDefault(tp, beginning);
            long start = Math.max(beginning, latest - tailCount);
            result.put(tp, start);
        }
        return result;
    }

    /** Resolve offsets for a position specification */
    static Map<TopicPartition, Long> resolveOffsets(KafkaConsumer<byte[], byte[]> consumer, Collection<TopicPartition> topicPartitions, String position, boolean isStart)
    {
        requireNonNull(position, "position");

        return switch (position.toLowerCase())
        {
            case "earliest" -> consumer.beginningOffsets(topicPartitions);
            case "latest" -> consumer.endOffsets(topicPartitions);
            default -> resolveCustomPosition(consumer, topicPartitions, position);
        };
    }

    private static Map<TopicPartition, Long> resolveCustomPosition(KafkaConsumer<byte[], byte[]> consumer, Collection<TopicPartition> topicPartitions, String position)
    {
        // Try numeric offset
        try
        {
            long offset = Long.parseLong(position);
            Map<TopicPartition, Long> result = new HashMap<>();
            for (TopicPartition tp : topicPartitions)
            {
                result.put(tp, offset);
            }
            return result;
        }
        catch (NumberFormatException e)
        {
            // Not numeric, try timestamp
        }

        // Try ISO timestamp
        try
        {
            long epochMillis = Instant.parse(position)
                    .toEpochMilli();
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition tp : topicPartitions)
            {
                timestampsToSearch.put(tp, epochMillis);
            }
            Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestampsToSearch);
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition tp : topicPartitions)
            {
                OffsetAndTimestamp oat = result.get(tp);
                if (oat != null)
                {
                    offsets.put(tp, oat.offset());
                }
                else
                {
                    offsets.put(tp, consumer.endOffsets(List.of(tp))
                            .getOrDefault(tp, 0L));
                }
            }
            return offsets;
        }
        catch (Exception e)
        {
            // Not a valid timestamp
        }

        throw new IllegalArgumentException("Invalid offset position: '" + position + "'. Expected: earliest, latest, numeric offset, or ISO timestamp");
    }
}
