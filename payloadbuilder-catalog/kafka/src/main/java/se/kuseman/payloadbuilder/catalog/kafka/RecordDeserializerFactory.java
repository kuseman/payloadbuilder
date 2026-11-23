package se.kuseman.payloadbuilder.catalog.kafka;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Factory for creating record deserializers based on format option */
class RecordDeserializerFactory
{
    private RecordDeserializerFactory()
    {
    }

    /** Create a deserializer for the given format */
    static IRecordDeserializer create(KafkaOptions.Format format, IQuerySession session, String catalogAlias)
    {
        return switch (format)
        {
            case AUTO -> new AutoRecordDeserializer(getSchemaRegistryUrl(session, catalogAlias));
            case JSON -> new JsonRecordDeserializer();
            case RAW -> new RawRecordDeserializer();
            case AVRO -> new AvroRecordDeserializer(getSchemaRegistryUrl(session, catalogAlias));
        };
    }

    /** Return the catalog's schema registry URL, or null if it isn't set */
    private static String getSchemaRegistryUrl(IQuerySession session, String catalogAlias)
    {
        ValueVector registryUrl = session.getCatalogProperty(catalogAlias, KafkaCatalog.SCHEMA_REGISTRY_URL);
        return (registryUrl != null
                && !registryUrl.isNull(0)) ? registryUrl.valueAsString(0)
                        : null;
    }
}
