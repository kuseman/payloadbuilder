package se.kuseman.payloadbuilder.catalog.kafka;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * A ValueVector that lazily deserializes raw byte payloads on access. This allows filters on metadata columns (partition, offset, timestamp) to execute without triggering payload deserialization.
 */
class LazyDeserializingValueVector implements ValueVector
{
    private final byte[][] rawPayloads;
    private final Object[] cache;
    private final boolean[] deserialized;
    private final IRecordDeserializer deserializer;

    LazyDeserializingValueVector(byte[][] rawPayloads, IRecordDeserializer deserializer)
    {
        this.rawPayloads = rawPayloads;
        this.deserializer = deserializer;
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
            cache[row] = deserializer.deserializeValue(rawPayloads[row]);
            deserialized[row] = true;
        }
        return cache[row];
    }
}
