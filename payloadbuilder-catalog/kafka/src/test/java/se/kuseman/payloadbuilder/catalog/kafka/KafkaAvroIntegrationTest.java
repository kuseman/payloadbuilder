package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.catalog.HttpClientUtils;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;

/** Integration test for Kafka catalog Avro decoding using Testcontainers with Schema Registry */
// CSOFF
class KafkaAvroIntegrationTest
// CSON
{
    private static final String CATALOG_ALIAS = "kafka";
    private static final String AVRO_TOPIC = "test_avro_orders";
    private static final int NUM_MESSAGES = 10;
    private static final int KAFKA_PORT = 9093;
    private static final int HOST_PORT = 19094;
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String AVRO_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.test\",\"fields\":[" + "{\"name\":\"orderId\",\"type\":\"int\"},"
                                                   + "{\"name\":\"customer\",\"type\":\"string\"},"
                                                   + "{\"name\":\"amount\",\"type\":\"double\"},"
                                                   + "{\"name\":\"active\",\"type\":\"boolean\"}"
                                                   + "]}";

    private static String bootstrapServers;
    private static String schemaRegistryUrl;
    private static GenericContainer<?> kafkaContainer;
    private static GenericContainer<?> schemaRegistryContainer;
    private static Network network;
    private static boolean available = false;

    @SuppressWarnings("resource")
    @BeforeAll
    static void setup() throws Exception
    {
        try
        {
            network = Network.newNetwork();

            kafkaContainer = new GenericContainer<>(DockerImageName.parse("apache/kafka:3.7.0")).withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                            .withPortBindings(new com.github.dockerjava.api.model.PortBinding(com.github.dockerjava.api.model.Ports.Binding.bindPort(HOST_PORT),
                                    new com.github.dockerjava.api.model.ExposedPort(KAFKA_PORT))))
                    .withExposedPorts(KAFKA_PORT)
                    .withEnv("KAFKA_NODE_ID", "1")
                    .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
                    .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9094,EXTERNAL://0.0.0.0:" + KAFKA_PORT)
                    .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092,EXTERNAL://localhost:" + HOST_PORT)
                    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT")
                    .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                    .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                    .waitingFor(Wait.forListeningPort());

            kafkaContainer.start();
            bootstrapServers = "localhost:" + HOST_PORT;
            Thread.sleep(3000);

            schemaRegistryContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.0")).withNetwork(network)
                    .withExposedPorts(SCHEMA_REGISTRY_PORT)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                    .waitingFor(Wait.forHttp("/subjects")
                            .forStatusCode(200));

            schemaRegistryContainer.start();
            schemaRegistryUrl = "http://localhost:" + schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT);
            available = true;

            createTopicAndProduce();
        }
        catch (Exception e)
        {
            System.err.println("Docker/Schema Registry not available: " + e.getMessage());
            e.printStackTrace();
            available = false;
        }
    }

    @AfterAll
    static void tearDown()
    {
        if (schemaRegistryContainer != null)
        {
            schemaRegistryContainer.stop();
        }
        if (kafkaContainer != null)
        {
            kafkaContainer.stop();
        }
        if (network != null)
        {
            network.close();
        }
    }

    private static void createTopicAndProduce() throws Exception
    {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        try (AdminClient admin = AdminClient.create(adminProps))
        {
            admin.createTopics(List.of(new NewTopic(AVRO_TOPIC, 1, (short) 1)))
                    .all()
                    .get();
        }

        int schemaId = registerSchema(AVRO_TOPIC + "-value", AVRO_SCHEMA_JSON);
        Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        produceAvroMessages(avroSchema, schemaId);
    }

    private static int registerSchema(String subject, String schemaJson) throws IOException
    {
        return registerSchema(subject, schemaJson, null);
    }

    private static int registerSchema(String subject, String schemaJson, String schemaType) throws IOException
    {
        String payload = MAPPER.writeValueAsString(schemaType == null ? Map.of("schema", schemaJson)
                : Map.of("schema", schemaJson, "schemaType", schemaType));
        HttpPost post = new HttpPost(schemaRegistryUrl + "/subjects/" + subject + "/versions");
        post.setEntity(new StringEntity(payload, org.apache.hc.core5.http.ContentType.APPLICATION_JSON));

        return HttpClientUtils.execute(null, post, null, null, null, null, null, null, response ->
        {
            String body = IOUtils.toString(response.getEntity()
                    .getContent(), StandardCharsets.UTF_8);
            @SuppressWarnings("unchecked")
            Map<String, Object> result = MAPPER.readValue(body, Map.class);
            return (Integer) result.get("id");
        });
    }

    private static void produceAvroMessages(Schema schema, int schemaId) throws Exception
    {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps))
        {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
                GenericRecord record = new GenericData.Record(schema);
                record.put("orderId", i);
                record.put("customer", "customer-" + i);
                record.put("amount", i * 10.5);
                record.put("active", i % 2 == 0);

                byte[] avroBytes = serializeAvro(record, schema);
                byte[] wireFormatBytes = wrapConfluentWireFormat(schemaId, avroBytes);

                String key = "avro-key-" + i;
                futures.add(producer.send(new ProducerRecord<>(AVRO_TOPIC, key.getBytes(StandardCharsets.UTF_8), wireFormatBytes)));
            }
            for (Future<RecordMetadata> f : futures)
            {
                f.get();
            }
        }
    }

    private static byte[] serializeAvro(GenericRecord record, Schema schema) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get()
                .binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static byte[] wrapConfluentWireFormat(int schemaId, byte[] avroBytes)
    {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + avroBytes.length);
        buffer.put((byte) 0x00);
        buffer.putInt(schemaId);
        buffer.put(avroBytes);
        return buffer.array();
    }

    private IExecutionContext createContext()
    {
        return TestUtils.mockExecutionContext(CATALOG_ALIAS, Map.of(KafkaCatalog.BOOTSTRAP_SERVERS, bootstrapServers, KafkaCatalog.SCHEMA_REGISTRY_URL, schemaRegistryUrl), 0, new KafkaNodeData());
    }

    private IExecutionContext createContextWithoutRegistry()
    {
        return TestUtils.mockExecutionContext(CATALOG_ALIAS, Map.of(KafkaCatalog.BOOTSTRAP_SERVERS, bootstrapServers), 0, new KafkaNodeData());
    }

    @Test
    void test_auto_format_reads_avro_topic_same_as_avro_format()
    {
        assumeTrue(available, "Docker/Schema Registry not available");

        List<Option> avroOptions = new ArrayList<>();
        avroOptions.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));

        Map<Long, Object> avro = readValuesByOffset(createContext(), AVRO_TOPIC, avroOptions);
        Map<Long, Object> auto = readValuesByOffset(createContext(), AVRO_TOPIC, new ArrayList<>());

        assertEquals(NUM_MESSAGES, avro.size());
        assertEquals(avro, auto);
        assertTrue(auto.get(0L) instanceof Map);
    }

    @Test
    void test_auto_format_reads_json_schema_values() throws Exception
    {
        assumeTrue(available, "Docker/Schema Registry not available");

        String topic = "json_schema_orders_" + System.nanoTime();
        int jsonSchemaId = registerSchema(topic + "-value", "{\"type\":\"object\",\"properties\":{\"orderId\":{\"type\":\"integer\"}}}", "JSON");
        int avroSchemaId = registerSchema(AVRO_TOPIC + "-value", AVRO_SCHEMA_JSON);
        Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("orderId", 7);
        record.put("customer", "customer-7");
        record.put("amount", 1.5);
        record.put("active", true);

        produce(topic,
                List.of(wrapConfluentWireFormat(jsonSchemaId, "{\"orderId\":1}".getBytes(StandardCharsets.UTF_8)),
                        wrapConfluentWireFormat(jsonSchemaId, "{\"orderId\":2}".getBytes(StandardCharsets.UTF_8)), wrapConfluentWireFormat(avroSchemaId, serializeAvro(record, avroSchema)),
                        "plain text".getBytes(StandardCharsets.UTF_8)));

        // With a registry: routed on schemaType
        Map<Long, Object> values = readValuesByOffset(createContext(), topic, new ArrayList<>());
        assertEquals(4, values.size());
        assertEquals(Map.of("orderId", 1), values.get(0L));
        assertEquals(Map.of("orderId", 2), values.get(1L));
        assertEquals(Map.of("orderId", 7, "customer", UTF8String.from("customer-7"), "amount", 1.5, "active", true), values.get(2L));
        assertEquals(UTF8String.from("plain text"), values.get(3L));

        // Without a registry: JSON Schema values are still JSON after the header
        Map<Long, Object> noRegistry = readValuesByOffset(createContextWithoutRegistry(), topic, new ArrayList<>());
        assertEquals(Map.of("orderId", 1), noRegistry.get(0L));
        assertTrue(noRegistry.get(2L) instanceof UTF8String);

        // Explicit avro: the JSON Schema value fails with a clear message
        List<Option> avroOptions = new ArrayList<>();
        avroOptions.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));
        RuntimeException e = assertThrows(RuntimeException.class, () -> readValuesByOffset(createContext(), topic, avroOptions));
        assertEquals("Value deserialization error at 0:0", e.getMessage());
        assertEquals("Schema id " + jsonSchemaId + " is JSON, not Avro", e.getCause()
                .getMessage());
    }

    private static void produce(String topic, List<byte[]> values) throws Exception
    {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (AdminClient admin = AdminClient.create(adminProps); KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps))
        {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all()
                    .get();
            for (int i = 0; i < values.size(); i++)
            {
                producer.send(new ProducerRecord<>(topic, 0, ("key-" + i).getBytes(StandardCharsets.UTF_8), values.get(i)))
                        .get();
            }
        }
    }

    /** Read every value of the topic, keyed by offset */
    private static Map<Long, Object> readValuesByOffset(IExecutionContext context, String topic, List<Option> options)
    {
        KafkaCatalog catalog = new KafkaCatalog();
        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", topic),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        Map<Long, Object> result = new HashMap<>();
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    long offset = batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getLong(i);
                    result.put(offset, batch.getColumn(KafkaTupleIterator.COL_VALUE)
                            .getAny(i));
                }
            }
        }
        finally
        {
            it.close();
        }
        return result;
    }

    @Test
    void test_scan_topic_avro_format()
    {
        assumeTrue(available, "Docker/Schema Registry not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", AVRO_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        int totalRows = 0;
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                assertNotNull(batch.getSchema());
                assertTrue(batch.getRowCount() > 0);

                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    // Key should be string
                    Object key = batch.getColumn(KafkaTupleIterator.COL_KEY)
                            .getAny(i);
                    assertNotNull(key);
                    assertTrue(key.toString()
                            .startsWith("avro-key-"));

                    // Value should be a Map (Avro GenericRecord decoded to Map)
                    Object value = batch.getColumn(KafkaTupleIterator.COL_VALUE)
                            .getAny(i);
                    assertNotNull(value);
                    assertTrue(value instanceof Map, "Expected Map but got: " + value.getClass()
                            .getName());

                    @SuppressWarnings("unchecked")
                    Map<String, Object> valueMap = (Map<String, Object>) value;

                    // Verify all Avro fields are present and correctly typed
                    assertTrue(valueMap.containsKey("orderId"), "Missing 'orderId' field");
                    assertTrue(valueMap.get("orderId") instanceof Integer, "orderId should be Integer");

                    assertTrue(valueMap.containsKey("customer"), "Missing 'customer' field");
                    assertTrue(valueMap.get("customer") instanceof UTF8String, "customer should be UTF8String");

                    assertTrue(valueMap.containsKey("amount"), "Missing 'amount' field");
                    assertTrue(valueMap.get("amount") instanceof Double, "amount should be Double");

                    assertTrue(valueMap.containsKey("active"), "Missing 'active' field");
                    assertTrue(valueMap.get("active") instanceof Boolean, "active should be Boolean");

                    int orderId = (int) valueMap.get("orderId");
                    assertTrue(orderId >= 0
                            && orderId < NUM_MESSAGES);
                    assertEquals("customer-" + orderId, valueMap.get("customer")
                            .toString());
                    assertEquals(orderId * 10.5, (double) valueMap.get("amount"), 0.001);
                    assertEquals(orderId % 2 == 0, valueMap.get("active"));
                }

                totalRows += batch.getRowCount();
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(NUM_MESSAGES, totalRows);
    }

    @Test
    void test_avro_lazy_value_not_deserialized_when_only_metadata_accessed()
    {
        assumeTrue(available, "Docker/Schema Registry not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("avro")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", AVRO_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        int totalRows = 0;
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    // Only access metadata columns, NOT the value column
                    Object key = batch.getColumn(KafkaTupleIterator.COL_KEY)
                            .getAny(i);
                    assertNotNull(key);

                    long offset = batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i) instanceof Long l ? l
                                    : -1;
                    assertTrue(offset >= 0);

                    int partition = batch.getColumn(KafkaTupleIterator.COL_PARTITION)
                            .getAny(i) instanceof Integer p ? p
                                    : -1;
                    assertEquals(0, partition);
                }
                totalRows += batch.getRowCount();
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(NUM_MESSAGES, totalRows);
    }
}
