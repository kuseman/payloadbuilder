package se.kuseman.payloadbuilder.catalog.kafka;

/** Deserializes Kafka record key and value bytes into objects */
interface IRecordDeserializer extends AutoCloseable
{
    /** Deserialize key bytes. Returns null for null input. */
    Object deserializeKey(byte[] keyBytes);

    /** Deserialize value bytes. Returns null for null input. */
    Object deserializeValue(byte[] valueBytes);

    @Override
    default void close()
    {
    }
}
