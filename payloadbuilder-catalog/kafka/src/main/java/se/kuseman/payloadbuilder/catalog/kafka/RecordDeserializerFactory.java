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
            case JSON -> new JsonRecordDeserializer();
            case RAW -> new RawRecordDeserializer();
            case AVRO ->
            {
                ValueVector registryUrl = session.getCatalogProperty(catalogAlias, KafkaCatalog.SCHEMA_REGISTRY_URL);
                String url = (registryUrl != null
                        && !registryUrl.isNull(0)) ? registryUrl.valueAsString(0)
                                : null;
                yield new AvroRecordDeserializer(url);
            }
        };
    }
}
