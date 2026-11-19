package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.catalog.TestUtils;

/** Test of {@link KafkaConsumerFactory} */
class KafkaConsumerFactoryTest
{
    @Test
    void test_viewer_mode_properties()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.BOOTSTRAP_SERVERS, "localhost:9092"), 0, null);

        Properties props = KafkaConsumerFactory.buildProperties(context.getSession(), "kafka");

        assertEquals("localhost:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("false", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("none", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertTrue(String.valueOf(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                .contains("ByteArrayDeserializer"));
        assertTrue(String.valueOf(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
                .contains("ByteArrayDeserializer"));

        // Group ID should be auto-generated
        assertNotNull(props.get(ConsumerConfig.GROUP_ID_CONFIG), "GROUP_ID should be set. Props: " + props);
    }

    @Test
    void test_security_properties()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.BOOTSTRAP_SERVERS, "localhost:9092", KafkaCatalog.SECURITY_PROTOCOL, "SASL_SSL",
                KafkaCatalog.SASL_MECHANISM, "PLAIN", KafkaCatalog.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required;"), 0, null);

        Properties props = KafkaConsumerFactory.buildProperties(context.getSession(), "kafka");

        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required;", props.get("sasl.jaas.config"));
    }

    @Test
    void test_missing_bootstrap_servers()
    {
        IExecutionContext context = TestUtils.mockExecutionContext("kafka", Map.of(), 0, null);

        assertThrows(IllegalArgumentException.class, () -> KafkaConsumerFactory.buildProperties(context.getSession(), "kafka"));
    }
}
