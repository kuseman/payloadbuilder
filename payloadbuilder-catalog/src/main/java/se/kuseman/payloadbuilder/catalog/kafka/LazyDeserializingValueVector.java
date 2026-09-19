package se.kuseman.payloadbuilder.catalog.kafka;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.kafka.KafkaOptions.OnError;

/**
 * A ValueVector that lazily deserializes raw byte payloads on access. This allows filters on metadata columns (partition, offset, timestamp) to execute without triggering payload deserialization.
 *
 * <pre>
 * Honors on_error='fail' (rethrow) and on_error='null' (swallow, cache null) on first access. on_error='skip' can't be
 * expressed here without changing the vector's row count out from under the other columns of the same batch, so rows
 * that fail deserialization under 'skip' must instead be dropped eagerly before this vector is built.
 * </pre>
 */
class LazyDeserializingValueVector implements ValueVector
{
    private final byte[][] rawPayloads;
    private final Object[] cache;
    private final boolean[] deserialized;
    private final IRecordDeserializer deserializer;
    private final OnError onError;
    private final int[] partitions;
    private final long[] offsets;
    private final KafkaNodeData nodeData;

    LazyDeserializingValueVector(byte[][] rawPayloads, IRecordDeserializer deserializer, OnError onError, int[] partitions, long[] offsets, KafkaNodeData nodeData)
    {
        this.rawPayloads = rawPayloads;
        this.deserializer = deserializer;
        this.onError = onError;
        this.partitions = partitions;
        this.offsets = offsets;
        this.nodeData = nodeData;
        this.cache = new Object[rawPayloads.length];
        this.deserialized = new boolean[rawPayloads.length];
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.ANY;
    }

    @Override
    public int size()
    {
        return rawPayloads.length;
    }

    @Override
    public boolean isNull(int row)
    {
        return rawPayloads[row] == null;
    }

    @Override
    public Object getAny(int row)
    {
        if (rawPayloads[row] == null)
        {
            return null;
        }
        if (!deserialized[row])
        {
            try
            {
                cache[row] = deserializer.deserializeValue(rawPayloads[row]);
            }
            catch (Exception e)
            {
                nodeData.deserializationErrors++;
                if (onError == OnError.FAIL)
                {
                    throw new RuntimeException("Value deserialization error at " + partitions[row] + ":" + offsets[row], e);
                }
                cache[row] = null;
            }
            deserialized[row] = true;
        }
        return cache[row];
    }
}
