package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.ExecutionMode;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.Format;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.SortOrder;

/** Test of {@link KafkaSplitResolver} */
@SuppressWarnings("unchecked")
class KafkaSplitResolverTest
{
    @Test
    void test_earliest_to_latest_batch()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null), new PartitionInfo("orders", 1, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L, tp1, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 100L, tp1, 200L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        assertEquals(2, splits.size());
        assertEquals(new KafkaSplit("orders", 0, 0, 100), splits.get(0));
        assertEquals(new KafkaSplit("orders", 1, 0, 200), splits.get(1));
    }

    @Test
    void test_numeric_offset_start()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 500L));

        KafkaOptions options = new KafkaOptions("100", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        assertEquals(1, splits.size());
        assertEquals(new KafkaSplit("orders", 0, 100, 500), splits.get(0));
    }

    @Test
    void test_timestamp_based_start()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.offsetsForTimes(any(Map.class))).thenReturn(Map.of(tp0, new OffsetAndTimestamp(50, 1704067200000L)));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 200L));

        KafkaOptions options = new KafkaOptions("2024-01-01T00:00:00Z", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        assertEquals(1, splits.size());
        assertEquals(50, splits.get(0)
                .startOffset());
        assertEquals(200, splits.get(0)
                .endOffset());
    }

    @Test
    void test_stream_mode_unbounded()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.STREAM, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        assertEquals(1, splits.size());
        assertEquals(Long.MAX_VALUE, splits.get(0)
                .endOffset());
    }

    @Test
    void test_empty_topic()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        // Empty split (start >= end) should be filtered out
        assertTrue(splits.isEmpty());
    }

    @Test
    void test_unknown_topic()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(null);

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "nonexistent", options);

        assertTrue(splits.isEmpty());
    }

    @Test
    void test_invalid_position()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        KafkaOptions options = new KafkaOptions("invalid_position", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        assertThrows(IllegalArgumentException.class, () -> KafkaSplitResolver.resolve(consumer, "orders", options));
    }

    // --- Predicate narrowing tests ---

    @Test
    void test_partition_filter_narrows_to_single_partition()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp1 = new TopicPartition("orders", 1);

        when(consumer.partitionsFor(any(), any(Duration.class)))
                .thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null), new PartitionInfo("orders", 1, null, null, null), new PartitionInfo("orders", 2, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp1, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp1, 100L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();
        analysis.partitionFilter = java.util.List.of(se.kuseman.payloadbuilder.test.IPredicateMock.expression(1));

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options, analysis, context);

        assertEquals(1, splits.size());
        assertEquals(1, splits.get(0)
                .partition());
    }

    @Test
    void test_offset_gte_narrows_start()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 1000L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();
        analysis.offsetLower = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(500L), true);

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options, analysis, context);

        assertEquals(1, splits.size());
        assertEquals(500, splits.get(0)
                .startOffset());
        assertEquals(1000, splits.get(0)
                .endOffset());
    }

    @Test
    void test_offset_lt_narrows_end()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 1000L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();
        analysis.offsetUpper = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(200L), false);

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options, analysis, context);

        assertEquals(1, splits.size());
        assertEquals(0, splits.get(0)
                .startOffset());
        assertEquals(200, splits.get(0)
                .endOffset());
    }

    @Test
    void test_combined_partition_and_offset_predicates()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null), new PartitionInfo("orders", 1, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 500L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();
        analysis.partitionFilter = java.util.List.of(se.kuseman.payloadbuilder.test.IPredicateMock.expression(0));
        analysis.offsetLower = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(100L), true);
        analysis.offsetUpper = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(300L), false);

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options, analysis, context);

        assertEquals(1, splits.size());
        assertEquals(0, splits.get(0)
                .partition());
        assertEquals(100, splits.get(0)
                .startOffset());
        assertEquals(300, splits.get(0)
                .endOffset());
    }

    @Test
    void test_offset_predicates_can_produce_empty_split()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null)));

        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 1000L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.OLDEST, 500);

        KafkaPredicateAnalysis analysis = new KafkaPredicateAnalysis();
        analysis.offsetLower = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(500L), true);
        analysis.offsetUpper = new se.kuseman.payloadbuilder.catalog.kafka.KafkaPredicateAnalysis.Bound(se.kuseman.payloadbuilder.test.IPredicateMock.expression(100L), false);

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options, analysis, context);

        assertTrue(splits.isEmpty(), "Impossible offset range should produce no splits");
    }

    @Test
    void test_newest_sort_uses_tail_window()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);

        when(consumer.partitionsFor(any(), any(Duration.class))).thenReturn(List.of(new PartitionInfo("orders", 0, null, null, null), new PartitionInfo("orders", 1, null, null, null)));
        when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(tp0, 0L, tp1, 100L));
        when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(tp0, 1000L, tp1, 120L));

        KafkaOptions options = new KafkaOptions("earliest", "latest", ExecutionMode.BATCH, Format.JSON, OnError.FAIL, 1000, 500, SortOrder.NEWEST, 50);

        List<KafkaSplit> splits = KafkaSplitResolver.resolve(consumer, "orders", options);

        assertEquals(2, splits.size());
        assertEquals(new KafkaSplit("orders", 0, 950, 1000), splits.get(0));
        assertEquals(new KafkaSplit("orders", 1, 100, 120), splits.get(1));
    }
}
