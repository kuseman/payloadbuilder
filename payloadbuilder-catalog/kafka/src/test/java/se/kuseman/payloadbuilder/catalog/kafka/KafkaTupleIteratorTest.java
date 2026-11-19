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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
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
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), mock(IRecordDeserializer.class), new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null, context, () ->
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
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), mock(IRecordDeserializer.class), new KafkaNodeData(), 10, false, OnError.FAIL, SortOrder.OLDEST, null, context, () ->
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
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split0, split1), mock(IRecordDeserializer.class), new KafkaNodeData(), 3, false, OnError.FAIL, SortOrder.OLDEST, null, context,
                () ->
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
        KafkaTupleIterator it = new KafkaTupleIterator(consumer, List.of(split), deserializer, nodeData, 10, false, OnError.SKIP, SortOrder.OLDEST, null, context, () ->
        {
        }, 1);

        assertTrue(it.hasNext());
        TupleVector batch = it.next();
        assertEquals(2, batch.getRowCount(), "The malformed row must be dropped entirely, not just nulled out");
        assertEquals(1, nodeData.deserializationErrors);
        assertFalse(it.hasNext());
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
