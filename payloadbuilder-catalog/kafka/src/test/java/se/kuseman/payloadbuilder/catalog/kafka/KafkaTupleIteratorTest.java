package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.SortOrder;

class KafkaTupleIteratorTest
{
    @Test
    void test_column_ordinals()
    {
        record Col(String name, int ordinal)
        {
        }

        Arrays.stream(KafkaTupleIterator.class.getDeclaredFields())
                .filter(f -> f.getName()
                        .startsWith("COL_"))
                .map(f -> new Col(f.getName()
                        .substring(4)
                        .toLowerCase(), readField(f)))
                .forEach(c -> assertEquals(c.ordinal, indexOf(c.name)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_estimated_row_count()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, new KafkaNodeData());
        Runnable noop = () ->
        {
        };

        // Bounded splits => sum of offset spans, so a hash join can hash the (smaller) other side instead of buffering the topic
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(new KafkaSplit("orders", 0, 10, 1_010), new KafkaSplit("orders", 1, 0, 500)), mock(IRecordDeserializer.class),
                new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null, KafkaProjection.ALL, context, noop, 1);
        assertEquals(1_500, it.estimatedRowCount());

        // Exceeding int range is capped
        it = new KafkaTupleIterator(consumer, List.of(new KafkaSplit("orders", 0, 0, Integer.MAX_VALUE), new KafkaSplit("orders", 1, 0, Integer.MAX_VALUE)), mock(IRecordDeserializer.class),
                new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null, KafkaProjection.ALL, context, noop, 1);
        assertEquals(Integer.MAX_VALUE, it.estimatedRowCount());

        // Any unbounded split => unknown
        it = new KafkaTupleIterator(consumer, List.of(new KafkaSplit("orders", 0, 0, 100), new KafkaSplit("orders", 1, 0, Long.MAX_VALUE)), mock(IRecordDeserializer.class), new KafkaNodeData(), 10,
                false, OnError.FAIL, SortOrder.OLDEST, null, KafkaProjection.ALL, context, noop, 1);
        assertEquals(-1, it.estimatedRowCount());

        // Stream mode => unknown
        it = new KafkaTupleIterator(consumer, List.of(new KafkaSplit("orders", 0, 0, 100)), mock(IRecordDeserializer.class), new KafkaNodeData(), 10, true, OnError.FAIL, SortOrder.OLDEST, null,
                KafkaProjection.ALL, context, noop, 1);
        assertEquals(-1, it.estimatedRowCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_batch_scan_survives_empty_polls_that_still_make_progress()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        KafkaSplit split = new KafkaSplit("orders", 0, 0, 5);

        // Four consecutive empty polls in a row - more than the old MAX_EMPTY_POLLS=3 giveup threshold - each
        // still advancing the fetch position (e.g. skipping over transaction control/marker offsets), followed
        // by a poll that surfaces the remaining real records and completes the split.
        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.<byte[], byte[]>empty(), ConsumerRecords.<byte[], byte[]>empty(), ConsumerRecords.<byte[], byte[]>empty(),
                ConsumerRecords.<byte[], byte[]>empty(), recordsOf(tp0, 1, 2, 3, 4));
        when(consumer.position(eq(tp0))).thenReturn(1L, 2L, 3L, 4L);

        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, new KafkaNodeData());
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), mock(IRecordDeserializer.class), new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null,
                KafkaProjection.ALL, context, () ->
                {
                }, 1);

        assertTrue(it.hasNext());
        TupleVector batch = it.next();
        assertEquals(4, batch.getRowCount());
        assertFalse(it.hasNext());
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_batch_scan_fails_fast_when_stalled_with_no_progress()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        KafkaSplit split = new KafkaSplit("orders", 0, 0, 5);

        // Every poll comes back empty and the fetch position never advances - a genuine stall (e.g. broker/network
        // issue) rather than a transient gap - so the scan must fail loudly instead of silently returning nothing.
        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.<byte[], byte[]>empty());
        when(consumer.position(eq(tp0))).thenReturn(0L);

        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, new KafkaNodeData());
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), mock(IRecordDeserializer.class), new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null,
                KafkaProjection.ALL, context, () ->
                {
                }, 1);

        assertThrows(IllegalStateException.class, it::hasNext);
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_batch_does_not_drop_records_when_a_single_poll_spans_multiple_partitions_past_batch_size()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);
        KafkaSplit split0 = new KafkaSplit("orders", 0, 0, 100);
        KafkaSplit split1 = new KafkaSplit("orders", 1, 0, 100);

        // A single poll() response spanning two partitions with more records (4) than fit in one batch (3). The
        // leftover record must carry over to the next batch instead of being silently discarded, since Kafka
        // won't hand back records already returned by poll().
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp0,
                List.of(new ConsumerRecord<byte[], byte[]>("orders", 0, 0, (byte[]) null, (byte[]) null), new ConsumerRecord<byte[], byte[]>("orders", 0, 1, (byte[]) null, (byte[]) null)), tp1,
                List.of(new ConsumerRecord<byte[], byte[]>("orders", 1, 0, (byte[]) null, (byte[]) null), new ConsumerRecord<byte[], byte[]>("orders", 1, 1, (byte[]) null, (byte[]) null))));

        when(consumer.poll(any(Duration.class))).thenReturn(records, ConsumerRecords.<byte[], byte[]>empty());

        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, new KafkaNodeData());
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split0, split1), mock(IRecordDeserializer.class), new KafkaNodeData(), 3, false, OnError.FAIL, SortOrder.OLDEST, null,
                KafkaProjection.ALL, context, () ->
                {
                }, 1);

        assertTrue(it.hasNext());
        int total = it.next()
                .getRowCount();
        assertTrue(it.hasNext());
        total += it.next()
                .getRowCount();

        assertEquals(4, total, "No record from the shared poll() response should be lost across the batch boundary");
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_on_error_skip_drops_rows_with_malformed_values()
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        KafkaSplit split = new KafkaSplit("orders", 0, 0, 3);

        byte[] good = "good".getBytes();
        byte[] bad = "bad".getBytes();

        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp0, List.of(new ConsumerRecord<byte[], byte[]>("orders", 0, 0, (byte[]) null, good),
                new ConsumerRecord<byte[], byte[]>("orders", 0, 1, (byte[]) null, bad), new ConsumerRecord<byte[], byte[]>("orders", 0, 2, (byte[]) null, good))));

        when(consumer.poll(any(Duration.class))).thenReturn(records);

        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);
        when(deserializer.deserializeValue(good)).thenReturn("ok");
        when(deserializer.deserializeValue(bad)).thenThrow(new RuntimeException("malformed"));

        KafkaNodeData nodeData = new KafkaNodeData();
        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, nodeData);
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), deserializer, nodeData, 10, false, OnError.SKIP, SortOrder.OLDEST, null, KafkaProjection.ALL, context, () ->
        {
        }, 1);

        assertTrue(it.hasNext());
        TupleVector batch = it.next();
        assertEquals(2, batch.getRowCount(), "The malformed row must be dropped entirely, not just nulled out");
        assertEquals(1, nodeData.deserializationErrors);
        assertFalse(it.hasNext());
    }

    @Test
    void test_projection_drops_value_and_headers_when_not_requested()
    {
        // Key-only projection (ie. a join on k.key): payload and headers are never retained
        TupleVector batch = scanSingleBatch(new KafkaProjection(false, false), SortOrder.OLDEST);
        assertEquals(2, batch.getRowCount());
        assertEquals("k1", batch.getColumn(KafkaTupleIterator.COL_KEY)
                .getAny(0));
        assertEquals(1L, batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                .getAny(1));
        assertEquals(UTF8String.from("orders"), batch.getColumn(KafkaTupleIterator.COL_TOPIC)
                .getAny(0));
        assertEquals(UTF8String.from("CreateTime"), batch.getColumn(KafkaTupleIterator.COL_TIMESTAMPTYPE)
                .getAny(0));
        assertTrue(batch.getColumn(KafkaTupleIterator.COL_VALUE)
                .isNull(0));
        assertTrue(batch.getColumn(KafkaTupleIterator.COL_HEADERS)
                .isNull(0));

        // Full projection keeps them
        batch = scanSingleBatch(KafkaProjection.ALL, SortOrder.OLDEST);
        assertEquals("value-k1", batch.getColumn(KafkaTupleIterator.COL_VALUE)
                .getAny(0));
        assertEquals(Map.of("h", "v"), batch.getColumn(KafkaTupleIterator.COL_HEADERS)
                .getAny(0));
        assertTrue(batch.getColumn(KafkaTupleIterator.COL_HEADERS)
                .isNull(1), "No headers => null");
    }

    @Test
    void test_sort_order_newest_reorders_all_columns_together()
    {
        TupleVector batch = scanSingleBatch(KafkaProjection.ALL, SortOrder.NEWEST);
        assertEquals(2, batch.getRowCount());
        // Newest (offset 1, timestamp 2000) first, every column must follow its row
        assertEquals(1L, batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                .getAny(0));
        assertEquals("k2", batch.getColumn(KafkaTupleIterator.COL_KEY)
                .getAny(0));
        assertEquals("value-k2", batch.getColumn(KafkaTupleIterator.COL_VALUE)
                .getAny(0));
        assertTrue(batch.getColumn(KafkaTupleIterator.COL_HEADERS)
                .isNull(0));
        assertEquals("k1", batch.getColumn(KafkaTupleIterator.COL_KEY)
                .getAny(1));
        assertEquals(Map.of("h", "v"), batch.getColumn(KafkaTupleIterator.COL_HEADERS)
                .getAny(1));
    }

    @SuppressWarnings("unchecked")
    private static TupleVector scanSingleBatch(KafkaProjection projection, SortOrder sortOrder)
    {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        RecordHeaders headers = new RecordHeaders();
        headers.add("h", "v".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> r0 = new ConsumerRecord<>("orders", 0, 0, 1000L, TimestampType.CREATE_TIME, 2, 8, "k1".getBytes(StandardCharsets.UTF_8),
                "value-k1".getBytes(StandardCharsets.UTF_8), headers, Optional.empty());
        ConsumerRecord<byte[], byte[]> r1 = new ConsumerRecord<>("orders", 0, 1, 2000L, TimestampType.CREATE_TIME, 2, 8, "k2".getBytes(StandardCharsets.UTF_8),
                "value-k2".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), Optional.empty());
        when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(tp0, List.of(r0, r1))));

        IRecordDeserializer deserializer = mock(IRecordDeserializer.class);
        when(deserializer.deserializeKey(any())).thenAnswer(i -> new String((byte[]) i.getArgument(0), StandardCharsets.UTF_8));
        when(deserializer.deserializeValue(any())).thenAnswer(i -> new String((byte[]) i.getArgument(0), StandardCharsets.UTF_8));

        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, new KafkaNodeData());
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(new KafkaSplit("orders", 0, 0, 2)), deserializer, new KafkaNodeData(), 10, false, OnError.FAIL, sortOrder, null, projection,
                context, () ->
                {
                }, 1);

        assertTrue(it.hasNext());
        TupleVector batch = it.next();
        assertFalse(it.hasNext());
        return batch;
    }

    private static ConsumerRecords<byte[], byte[]> recordsOf(TopicPartition tp, long... offsets)
    {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.stream(offsets)
                .mapToObj(offset -> new ConsumerRecord<byte[], byte[]>(tp.topic(), tp.partition(), offset, (byte[]) null, (byte[]) null))
                .toList();
        return new ConsumerRecords<>(Map.of(tp, records));
    }

    private int readField(Field field)
    {
        try
        {
            field.setAccessible(true);
            return (int) FieldUtils.readStaticField(field);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("Error reading field", e);
        }
    }

    private int indexOf(String column)
    {
        int size = KafkaTupleIterator.SCHEMA.getSize();
        for (int i = 0; i < size; i++)
        {
            if (column.equalsIgnoreCase(KafkaTupleIterator.SCHEMA.getColumns()
                    .get(i)
                    .getName()))
            {
                return i;
            }
        }
        throw new IllegalArgumentException("Column: " + column + " not found");
    }
}
