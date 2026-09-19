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
     * @param hasResidualPredicates True if the query has predicates that could not be pushed down and will instead be evaluated by the engine on the data this method returns. When true, the "newest"
     * tail-window narrowing is skipped since it would silently exclude matching records outside the tail window.
     * @return List of bounded KafkaSplits, one per partition. Empty splits are filtered out.
     */
    static List<KafkaSplit> resolve(KafkaConsumer<byte[], byte[]> consumer, String topic, KafkaOptions options, KafkaPredicateAnalysis predicateAnalysis, IExecutionContext context,
            boolean hasResidualPredicates)
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

        // Apply partition filter from predicate analysis. Every predicate is ANDed together so the target set is
        // the INTERSECTION of each predicate's allowed values, not their union - "partition = 1 AND partition = 2"
        // must resolve to no partitions at all, not {1, 2}.
        if (predicateAnalysis != null
                && predicateAnalysis.partitionFilter != null
                && context != null)
        {
            Set<Integer> targetPartitions = null;
            for (List<IExpression> group : predicateAnalysis.partitionFilter)
            {
                Set<Integer> groupValues = new HashSet<>();
                for (IExpression expr : group)
                {
                    ValueVector val = expr.eval(context);
                    if (val != null
                            && !val.isNull(0))
                    {
                        groupValues.add(val.getInt(0));
                    }
                }
                if (targetPartitions == null)
                {
                    targetPartitions = groupValues;
                }
                else
                {
                    targetPartitions.retainAll(groupValues);
                }
            }
            if (targetPartitions != null)
            {
                Set<Integer> finalTargetPartitions = targetPartitions;
                topicPartitions = topicPartitions.stream()
                        .filter(tp -> finalTargetPartitions.contains(tp.partition()))
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
                && options.sortOrder() == KafkaOptions.SortOrder.NEWEST
                && !hasResidualPredicates)
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
        return resolve(consumer, topic, options, null, null, false);
    }

    private static void applyOffsetPredicates(KafkaConsumer<byte[], byte[]> consumer, Map<TopicPartition, Long> startOffsets, Map<TopicPartition, Long> endOffsets,
            List<TopicPartition> topicPartitions, KafkaPredicateAnalysis analysis, IExecutionContext context)
    {
        // Offset lower bounds: offset >= N (inclusive) or offset > N (exclusive, +1). Every bound is ANDed
        // together so the strongest (highest) one wins.
        if (analysis.offsetLower != null)
        {
            for (KafkaPredicateAnalysis.Bound bound : analysis.offsetLower)
            {
                long value = evalLong(bound.expression(), context);
                long startOffset = bound.inclusive() ? value
                        : value + 1;
                for (TopicPartition tp : topicPartitions)
                {
                    startOffsets.merge(tp, startOffset, Math::max);
                }
            }
        }

        // Offset upper bounds: offset < N (exclusive) or offset <= N (inclusive, +1). Every bound is ANDed
        // together so the strongest (lowest) one wins.
        if (analysis.offsetUpper != null)
        {
            for (KafkaPredicateAnalysis.Bound bound : analysis.offsetUpper)
            {
                long value = evalLong(bound.expression(), context);
                long endOffset = bound.inclusive() ? value + 1
                        : value;
                for (TopicPartition tp : topicPartitions)
                {
                    endOffsets.merge(tp, endOffset, Math::min);
                }
            }
        }

        // Timestamp lower bounds: use offsetsForTimes to find start offset.
        // offsetsForTimes returns the earliest offset with timestamp >= given timestamp
        // For >= T: use offsetsForTimes(T)
        // For > T: use offsetsForTimes(T + 1)
        // Every bound is ANDed together so the strongest (highest resulting start offset) one wins.
        if (analysis.timestampLower != null)
        {
            Map<TopicPartition, Long> actualEndOffsets = null;
            for (KafkaPredicateAnalysis.Bound bound : analysis.timestampLower)
            {
                long value = evalLong(bound.expression(), context);
                long searchTs = bound.inclusive() ? value
                        : value + 1;
                Map<TopicPartition, Long> tsOffsets = resolveTimestampOffsets(consumer, topicPartitions, searchTs);
                for (TopicPartition tp : topicPartitions)
                {
                    Long tsOffset = tsOffsets.get(tp);
                    if (tsOffset != null)
                    {
                        startOffsets.merge(tp, tsOffset, Math::max);
                    }
                    else
                    {
                        // No record in this partition has a timestamp at or after the searched value, so the
                        // partition has nothing to contribute - force it empty by pushing the start to (at
                        // least) its real end offset rather than leaving the previous, wider start in place.
                        if (actualEndOffsets == null)
                        {
                            actualEndOffsets = consumer.endOffsets(topicPartitions);
                        }
                        Long actualEnd = actualEndOffsets.get(tp);
                        if (actualEnd != null)
                        {
                            startOffsets.merge(tp, actualEnd, Math::max);
                        }
                    }
                }
            }
        }

        // Timestamp upper bounds: use offsetsForTimes to find end offset.
        // For < T: use offsetsForTimes(T) as end (exclusive)
        // For <= T: use offsetsForTimes(T + 1) as end (exclusive)
        // Every bound is ANDed together so the strongest (lowest resulting end offset) one wins. When
        // offsetsForTimes finds no matching record, every record in the partition qualifies, so the end is left
        // unchanged.
        if (analysis.timestampUpper != null)
        {
            for (KafkaPredicateAnalysis.Bound bound : analysis.timestampUpper)
            {
                long value = evalLong(bound.expression(), context);
                long searchTs = bound.inclusive() ? value + 1
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

        Map<TopicPartition, Long> requested = resolveOffsets(consumer, topicPartitions, options.end(), false);
        if ("latest".equalsIgnoreCase(options.end()))
        {
            // Already the real high watermark, nothing to clamp
            return requested;
        }

        // A batch-mode scan is bounded by what's actually in the topic right now. Clamp any requested end
        // offset (a numeric offset or a resolved timestamp) that lies beyond the current high watermark, so a
        // too-generous "end" can't turn into an indefinite wait for records that were never going to arrive in
        // a bounded scan - unlike stream mode, batch mode has no reason to wait for the future.
        Map<TopicPartition, Long> actualEnd = consumer.endOffsets(topicPartitions);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : topicPartitions)
        {
            long requestedEnd = requested.getOrDefault(tp, Long.MAX_VALUE);
            long actual = actualEnd.getOrDefault(tp, requestedEnd);
            result.put(tp, Math.min(requestedEnd, actual));
        }
        return result;
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
