package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Creates AdminClient instances from session catalog properties */
class KafkaAdminFactory
{
    private KafkaAdminFactory()
    {
    }

    /** Create an AdminClient from session properties */
    static AdminClient createAdmin(IQuerySession session, String catalogAlias)
    {
        Properties props = new Properties();

        ValueVector bootstrapServers = session.getCatalogProperty(catalogAlias, KafkaCatalog.BOOTSTRAP_SERVERS);
        if (bootstrapServers == null
                || bootstrapServers.isNull(0))
        {
            throw new IllegalArgumentException("Missing required catalog property '" + KafkaCatalog.BOOTSTRAP_SERVERS + "'");
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.valueAsString(0));

        // Optional security properties
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SECURITY_PROTOCOL, "security.protocol", props);
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SASL_MECHANISM, "sasl.mechanism", props);
        setOptionalProperty(session, catalogAlias, KafkaCatalog.SASL_JAAS_CONFIG, "sasl.jaas.config", props);

        return AdminClient.create(props);
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
