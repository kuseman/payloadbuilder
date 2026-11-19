package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Creates KafkaConsumer instances with viewer-mode safety enforced */
class KafkaConsumerFactory
{
    private KafkaConsumerFactory()
    {
    }

    /** Create a viewer-mode consumer that never commits offsets */
    static KafkaConsumer<byte[], byte[]> createConsumer(IQuerySession session, String catalogAlias)
    {
        Properties props = buildProperties(session, catalogAlias);
        return new KafkaConsumer<>(props);
    }

    /** Build consumer properties from session catalog properties */
    static Properties buildProperties(IQuerySession session, String catalogAlias)
    {
        String bootstrapServers = getRequiredProperty(session, catalogAlias, KafkaCatalog.BOOTSTRAP_SERVERS);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Viewer-mode safety: never commit offsets, never join consumer groups
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "__payloadbuilder_" + UUID.randomUUID());

        // Raw byte deserialization - decoding is handled by IRecordDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Optional security properties
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SECURITY_PROTOCOL, "security.protocol", props);
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SASL_MECHANISM, "sasl.mechanism", props);
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SASL_JAAS_CONFIG, "sasl.jaas.config", props);

        return props;
    }

    private static String getRequiredProperty(IQuerySession session, String catalogAlias, String key)
    {
        ValueVector value = session.getCatalogProperty(catalogAlias, key);
        if (value == null
                || value.isNull(0))
        {
            throw new IllegalArgumentException("Missing required catalog property '" + key + "' for catalog alias '" + catalogAlias + "'");
        }
        return requireNonNull(value.valueAsString(0));
    }

    private static void setOptionalProperty(IQuerySession session, String catalogAlias, String catalogKey, String kafkaKey, Properties props)
    {
        ValueVector value = session.getCatalogProperty(catalogAlias, catalogKey);
        if (value != null
                && !value.isNull(0))
        {
            String strValue = value.valueAsString(0);
            if (strValue != null)
            {
                props.put(kafkaKey, strValue);
            }
        }
    }
}
