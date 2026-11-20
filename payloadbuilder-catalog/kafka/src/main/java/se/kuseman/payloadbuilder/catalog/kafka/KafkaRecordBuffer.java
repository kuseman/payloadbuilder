package se.kuseman.payloadbuilder.catalog.kafka;

import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_HEADERS;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_KEY;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_OFFSET;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_PARTITION;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_TIMESTAMP;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_TIMESTAMPTYPE;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_TOPIC;
import static se.kuseman.payloadbuilder.catalog.kafka.KafkaTupleIterator.COL_VALUE;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;

/**
 * Columnar, growable buffer of polled records. Uses primitive arrays and shared string instances so that a buffered record costs little more than its key - plus its value payload and headers only
 * when the {@link KafkaProjection} asks for them. This matters since anything downstream that buffers the scan (hash join, sort, temp table) keeps whole batches reachable.
 */
class KafkaRecordBuffer
{
    private static final UTF8String[] TIMESTAMP_TYPES = Arrays.stream(TimestampType.values())
            .map(t -> UTF8String.from(t.name))
            .toArray(UTF8String[]::new);

    private final KafkaProjection projection;
    private Object[] keys;
    private byte[][] values;
    private Object[] headers;
    private long[] offsets;
    private int[] partitions;
    private long[] timestamps;
    private byte[] timestampTypes;
    private int size;

    KafkaRecordBuffer(int capacity, KafkaProjection projection)
    {
        this.projection = projection;
        int cap = Math.max(capacity, 1);
        keys = new Object[cap];
        values = projection.value() ? new byte[cap][]
                : null;
        headers = projection.headers() ? new Object[cap]
                : null;
        offsets = new long[cap];
        partitions = new int[cap];
        timestamps = new long[cap];
        timestampTypes = new byte[cap];
    }

    int size()
    {
        return size;
    }

    long offset(int row)
    {
        return offsets[row];
    }

    int partition(int row)
    {
        return partitions[row];
    }

    long timestamp(int row)
    {
        return timestamps[row];
    }

    void add(ConsumerRecord<byte[], byte[]> record, Object key)
    {
        if (size == offsets.length)
        {
            grow();
        }
        keys[size] = key;
        if (values != null)
        {
            values[size] = record.value();
        }
        if (headers != null)
        {
            headers[size] = convertHeaders(record.headers());
        }
        offsets[size] = record.offset();
        partitions[size] = record.partition();
        timestamps[size] = record.timestamp();
        timestampTypes[size] = (byte) record.timestampType()
                .ordinal();
        size++;
    }

    /** Return a new buffer with rows [from, to) */
    KafkaRecordBuffer slice(int from, int to)
    {
        int length = to - from;
        KafkaRecordBuffer result = new KafkaRecordBuffer(length, projection);
        System.arraycopy(keys, from, result.keys, 0, length);
        if (values != null)
        {
            System.arraycopy(values, from, result.values, 0, length);
        }
        if (headers != null)
        {
            System.arraycopy(headers, from, result.headers, 0, length);
        }
        System.arraycopy(offsets, from, result.offsets, 0, length);
        System.arraycopy(partitions, from, result.partitions, 0, length);
        System.arraycopy(timestamps, from, result.timestamps, 0, length);
        System.arraycopy(timestampTypes, from, result.timestampTypes, 0, length);
        result.size = length;
        return result;
    }

    /** Physically reorder rows. Row {@code i} after the call is the row at {@code order[i]} before it. */
    void reorder(int[] order)
    {
        Object[] newKeys = new Object[size];
        byte[][] newValues = values != null ? new byte[size][]
                : null;
        Object[] newHeaders = headers != null ? new Object[size]
                : null;
        long[] newOffsets = new long[size];
        int[] newPartitions = new int[size];
        long[] newTimestamps = new long[size];
        byte[] newTimestampTypes = new byte[size];
        for (int i = 0; i < size; i++)
        {
            int from = order[i];
            newKeys[i] = keys[from];
            if (newValues != null)
            {
                newValues[i] = values[from];
            }
            if (newHeaders != null)
            {
                newHeaders[i] = headers[from];
            }
            newOffsets[i] = offsets[from];
            newPartitions[i] = partitions[from];
            newTimestamps[i] = timestamps[from];
            newTimestampTypes[i] = timestampTypes[from];
        }
        keys = newKeys;
        values = newValues;
        headers = newHeaders;
        offsets = newOffsets;
        partitions = newPartitions;
        timestamps = newTimestamps;
        timestampTypes = newTimestampTypes;
    }

    /** Build a tuple vector of this buffer. The buffer must not be modified afterwards since the vector shares its arrays. */
    TupleVector toTupleVector(UTF8String topic, IRecordDeserializer deserializer, OnError onError, KafkaNodeData nodeData)
    {
        // Drop unused capacity (partial batches) since the vector may be retained for a long time downstream.
        // LazyDeserializingValueVector also derives its size from the payload array length.
        KafkaRecordBuffer b = size == offsets.length ? this
                : slice(0, size);
        int rowCount = size;
        Object[] keys = b.keys;
        Object[] headers = b.headers;
        long[] offsets = b.offsets;
        int[] partitions = b.partitions;
        long[] timestamps = b.timestamps;
        byte[] timestampTypes = b.timestampTypes;

        // Lazy value column. on_error='skip' rows never reach here (dropped eagerly while polling), so this
        // only has to honor 'fail' (rethrow) and 'null' (swallow) on first access.
        ValueVector valueVector = b.values != null ? new LazyDeserializingValueVector(b.values, deserializer, onError, partitions, offsets, nodeData)
                : null;

        return new ObjectTupleVector(KafkaTupleIterator.SCHEMA, rowCount, (row, col) -> switch (col)
        {
            case COL_KEY -> keys[row];
            case COL_VALUE -> valueVector != null ? valueVector.getAny(row)
                    : null;
            case COL_OFFSET -> offsets[row];
            case COL_PARTITION -> partitions[row];
            case COL_TIMESTAMP -> EpochDateTimeOffset.from(timestamps[row]);
            case COL_TIMESTAMPTYPE -> TIMESTAMP_TYPES[timestampTypes[row]];
            case COL_HEADERS -> headers != null ? headers[row]
                    : null;
            case COL_TOPIC -> topic;
            default -> throw new IllegalArgumentException("Invalid column: " + col);
        });
    }

    private void grow()
    {
        int newCapacity = offsets.length * 2;
        keys = Arrays.copyOf(keys, newCapacity);
        if (values != null)
        {
            values = Arrays.copyOf(values, newCapacity);
        }
        if (headers != null)
        {
            headers = Arrays.copyOf(headers, newCapacity);
        }
        offsets = Arrays.copyOf(offsets, newCapacity);
        partitions = Arrays.copyOf(partitions, newCapacity);
        timestamps = Arrays.copyOf(timestamps, newCapacity);
        timestampTypes = Arrays.copyOf(timestampTypes, newCapacity);
    }

    private static Map<String, Object> convertHeaders(Headers kafkaHeaders)
    {
        if (kafkaHeaders == null)
        {
            return null;
        }
        Map<String, Object> result = null;
        for (Header header : kafkaHeaders)
        {
            if (result == null)
            {
                result = new HashMap<>();
            }
            result.put(header.key(), header.value() != null ? new String(header.value(), StandardCharsets.UTF_8)
                    : null);
        }
        return result;
    }
}
