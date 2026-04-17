package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.ThreadUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;

/** Integration test for Kafka catalog using Testcontainers (JSON, raw, metadata) */
// CSOFF
class KafkaIntegrationTest
// CSON
{
    private static final String CATALOG_ALIAS = "kafka";
    private static final String TEST_TOPIC = "test_orders";
    private static final int NUM_MESSAGES = 10;
    private static final int KAFKA_PORT = 9093;
    private static final int HOST_PORT = 19093;

    private static String bootstrapServers;
    private static GenericContainer<?> kafkaContainer;
    private static boolean dockerAvailable = false;

    @SuppressWarnings("resource")
    @BeforeAll
    static void setup() throws Exception
    {
        try
        {
            kafkaContainer = new GenericContainer<>(DockerImageName.parse("apache/kafka:3.7.0")).withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                    .withPortBindings(new com.github.dockerjava.api.model.PortBinding(com.github.dockerjava.api.model.Ports.Binding.bindPort(HOST_PORT),
                            new com.github.dockerjava.api.model.ExposedPort(KAFKA_PORT))))
                    .withExposedPorts(KAFKA_PORT)
                    .withEnv("KAFKA_NODE_ID", "1")
                    .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
                    .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9094,EXTERNAL://0.0.0.0:" + KAFKA_PORT)
                    .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092,EXTERNAL://localhost:" + HOST_PORT)
                    .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT")
                    .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                    .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                    .waitingFor(Wait.forListeningPort());

            kafkaContainer.start();
            bootstrapServers = "localhost:" + HOST_PORT;
            dockerAvailable = true;
            Thread.sleep(3000);

            createTopicAndProduce();
        }
        catch (Exception e)
        {
            System.err.println("Docker not available or Kafka container failed to start: " + e.getMessage());
            e.printStackTrace();
            dockerAvailable = false;
        }
    }

    @AfterAll
    static void tearDown()
    {
        if (kafkaContainer != null)
        {
            kafkaContainer.stop();
        }
    }

    private static void createTopicAndProduce() throws ExecutionException, InterruptedException
    {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        try (AdminClient admin = AdminClient.create(adminProps))
        {
            admin.createTopics(List.of(new NewTopic(TEST_TOPIC, 2, (short) 1)))
                    .all()
                    .get();
        }

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps))
        {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
                String key = "key-" + i;
                String value = "{\"orderId\":" + i + ",\"customer\":\"customer-" + i + "\",\"amount\":" + (i * 10.5) + "}";
                futures.add(producer.send(new ProducerRecord<>(TEST_TOPIC, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))));
            }
            for (Future<RecordMetadata> f : futures)
            {
                f.get();
            }
        }
    }

    private IExecutionContext createContext()
    {
        return createContext(new HashMap<>());
    }

    private IExecutionContext createContext(Map<String, Object> catalogProperties)
    {
        catalogProperties.put(KafkaCatalog.BOOTSTRAP_SERVERS, bootstrapServers);
        return TestUtils.mockExecutionContext(CATALOG_ALIAS, catalogProperties, 0, new KafkaNodeData());
    }

    @Test
    void test_scan_topic_earliest_to_latest()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
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

                assertEquals("key", batch.getSchema()
                        .getColumns()
                        .get(0)
                        .getName());
                assertEquals("value", batch.getSchema()
                        .getColumns()
                        .get(1)
                        .getName());

                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    Object key = batch.getColumn(KafkaTupleIterator.COL_KEY)
                            .getAny(i);
                    assertNotNull(key);
                    assertTrue(key.toString()
                            .startsWith("key-"));

                    Object value = batch.getColumn(KafkaTupleIterator.COL_VALUE)
                            .getAny(i);
                    assertNotNull(value);
                    assertTrue(value instanceof Map);
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
    void test_scan_topic_raw_format()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("raw")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
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
                    Object value = batch.getColumn(KafkaTupleIterator.COL_VALUE)
                            .getAny(i);
                    assertNotNull(value);
                    assertTrue(value.toString()
                            .contains("orderId"));
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
    void test_scan_topic_raw_format_topic_name_in_catalog_properties()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext(new HashMap<>(Map.of(KafkaCatalog.TOPIC, TEST_TOPIC)));

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.FORMAT, ExpressionTestUtils.createStringExpression("raw")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        int totalRows = 0;
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    Object value = batch.getColumn(KafkaTupleIterator.COL_VALUE)
                            .getAny(i);
                    assertNotNull(value);
                    assertTrue(value.toString()
                            .contains("orderId"));
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
    void test_scan_with_numeric_offset_bounds()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("0")));
        options.add(new Option(KafkaOptions.END, ExpressionTestUtils.createStringExpression("2")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
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
                    long offset = batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i) instanceof Long l ? l
                                    : -1;
                    assertTrue(offset >= 0
                            && offset < 2);
                }
                totalRows += batch.getRowCount();
            }
        }
        finally
        {
            it.close();
        }

        assertTrue(totalRows > 0
                && totalRows <= 4);
    }

    @Test
    void test_lazy_value_not_accessed()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    Object key = batch.getColumn(KafkaTupleIterator.COL_KEY)
                            .getAny(i);
                    assertNotNull(key);
                    long offset = batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i) instanceof Long l ? l
                                    : -1;
                    assertTrue(offset >= 0);
                }
            }
        }
        finally
        {
            it.close();
        }
    }

    @Test
    void test_metadata_topics()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("metadata", "topics"),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        try
        {
            assertTrue(it.hasNext());
            TupleVector batch = it.next();

            boolean foundTestTopic = false;
            for (int i = 0; i < batch.getRowCount(); i++)
            {
                String name = batch.getColumn(0)
                        .getAny(i)
                        .toString();
                if (TEST_TOPIC.equals(name))
                {
                    foundTestTopic = true;
                    int partitions = (int) batch.getColumn(1)
                            .getAny(i);
                    assertEquals(2, partitions);
                }
            }
            assertTrue(foundTestTopic, "Should find test topic");
        }
        finally
        {
            it.close();
        }
    }

    @Test
    void test_metadata_partitions()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("metadata", "partitions"),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        try
        {
            assertTrue(it.hasNext());
            TupleVector batch = it.next();

            int testTopicPartitions = 0;
            for (int i = 0; i < batch.getRowCount(); i++)
            {
                String topic = batch.getColumn(0)
                        .getAny(i)
                        .toString();
                if (TEST_TOPIC.equals(topic))
                {
                    testTopicPartitions++;
                }
            }
            assertEquals(2, testTopicPartitions);
        }
        finally
        {
            it.close();
        }
    }

    @Test
    void test_metadata_consumers()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props))
        {
            consumer.subscribe(List.of(TEST_TOPIC));
            consumer.poll(Duration.ofMillis(1000));
            ThreadUtils.sleepQuietly(Duration.ofMillis(500));
            KafkaCatalog catalog = new KafkaCatalog();
            IExecutionContext context = createContext();

            IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("metadata", "consumers"),
                    new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, emptyList()));

            TupleIterator it = ds.execute(context);
            try
            {
                assertTrue(it.hasNext());
                TupleVector batch = it.next();
                assertTrue(batch.getRowCount() >= 1);
                assertTrue(batch.toCsv()
                        .contains("test-group"));
            }
            finally
            {
                it.close();
            }
        }
    }

    @Test
    void test_consumer_group_offsets()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        String groupId = "test_cg_group";

        // Create a consumer group by consuming and committing offsets
        Properties consumerProps = new Properties();
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());

        try (org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> groupConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps))
        {
            // Commit offsets at position 3 for both partitions
            org.apache.kafka.common.TopicPartition tp0 = new org.apache.kafka.common.TopicPartition(TEST_TOPIC, 0);
            org.apache.kafka.common.TopicPartition tp1 = new org.apache.kafka.common.TopicPartition(TEST_TOPIC, 1);
            groupConsumer.commitSync(Map.of(tp0, new org.apache.kafka.clients.consumer.OffsetAndMetadata(3), tp1, new org.apache.kafka.clients.consumer.OffsetAndMetadata(3)));
        }

        // Query consumer group offsets via catalog
        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("consumer_group", groupId),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, emptyList()));

        TupleIterator it = ds.execute(context);
        try
        {
            assertTrue(it.hasNext());
            TupleVector batch = it.next();
            assertTrue(batch.getRowCount() > 0, "Should have consumer group offset rows");

            for (int i = 0; i < batch.getRowCount(); i++)
            {
                String topic = batch.getColumn(0)
                        .getAny(i)
                        .toString();
                assertEquals(TEST_TOPIC, topic);

                long currentOffset = (long) batch.getColumn(2)
                        .getAny(i);
                assertEquals(3, currentOffset, "Committed offset should be 3");

                long endOffset = (long) batch.getColumn(3)
                        .getAny(i);
                assertTrue(endOffset >= currentOffset, "End offset should be >= current offset");

                long lag = (long) batch.getColumn(4)
                        .getAny(i);
                assertEquals(endOffset - currentOffset, lag, "Lag should be end - current");
            }
        }
        finally
        {
            it.close();
        }
    }

    @Test
    void test_stream_mode_returns_data_without_hanging()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.MODE, ExpressionTestUtils.createStringExpression("stream")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        // Stream mode should return available data as partial batches without hanging
        int totalRows = 0;
        TupleIterator it = ds.execute(context);
        try
        {
            // Read first batch - should contain data and return promptly
            assertTrue(it.hasNext(), "Stream mode should return data");
            TupleVector batch = it.next();
            assertTrue(batch.getRowCount() > 0, "First batch should have rows");
            totalRows += batch.getRowCount();

            // Keep reading until we've consumed all existing messages
            // Stream mode won't terminate on its own, so we break after collecting all known messages
            while (totalRows < NUM_MESSAGES
                    && it.hasNext())
            {
                batch = it.next();
                totalRows += batch.getRowCount();
            }

            assertEquals(NUM_MESSAGES, totalRows, "Should read all messages in stream mode");
        }
        finally
        {
            it.close();
        }
    }

    @Test
    void test_batch_newest_sort_order_option()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("newest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        List<RowOrder> rows = new ArrayList<>();
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    rows.add(new RowOrder(getTimestampEpoch(batch, i), (long) batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i),
                            (int) batch.getColumn(KafkaTupleIterator.COL_PARTITION)
                                    .getAny(i)));
                }
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(NUM_MESSAGES, rows.size());
        assertNewestOrder(rows);
    }

    @Test
    void test_batch_newest_tail_count_limits_window()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("newest")));
        options.add(new Option(KafkaOptions.TAIL_COUNT, ExpressionTestUtils.createIntegerExpression(1)));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), emptyList(), Projection.ALL, options));

        int totalRows = 0;
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                totalRows += it.next()
                        .getRowCount();
            }
        }
        finally
        {
            it.close();
        }

        assertTrue(totalRows <= 2, "tail_count=1 should return at most one row per partition");
    }

    @Test
    void test_sort_item_timestamp_desc_pushdown_behaves_like_newest()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<ISortItem> sortItems = new ArrayList<>(List.of(TestUtils.mockSortItem(QualifiedName.of("timestamp"), ISortItem.Order.DESC)));
        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, options));

        List<RowOrder> rows = new ArrayList<>();
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    rows.add(new RowOrder(getTimestampEpoch(batch, i), (long) batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i),
                            (int) batch.getColumn(KafkaTupleIterator.COL_PARTITION)
                                    .getAny(i)));
                }
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(0, sortItems.size(), "Sort items should be consumed when pushed down");
        assertEquals(NUM_MESSAGES, rows.size());
        assertNewestOrder(rows);
    }

    @Test
    void test_sort_item_offset_desc_pushdown_behaves_like_newest()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<ISortItem> sortItems = new ArrayList<>(List.of(TestUtils.mockSortItem(QualifiedName.of("offset"), ISortItem.Order.DESC)));
        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, options));

        List<RowOrder> rows = new ArrayList<>();
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    rows.add(new RowOrder(getTimestampEpoch(batch, i), (long) batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i),
                            (int) batch.getColumn(KafkaTupleIterator.COL_PARTITION)
                                    .getAny(i)));
                }
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(0, sortItems.size(), "Sort items should be consumed when pushed down");
        assertEquals(NUM_MESSAGES, rows.size());
        assertNewestOrder(rows);
    }

    @Test
    void test_sort_order_option_newest_applies_when_sort_item_not_pushable()
    {
        assumeTrue(dockerAvailable, "Docker not available");

        KafkaCatalog catalog = new KafkaCatalog();
        IExecutionContext context = createContext();

        List<ISortItem> sortItems = new ArrayList<>(List.of(TestUtils.mockSortItem(QualifiedName.of("key"), ISortItem.Order.ASC)));
        List<Option> options = new ArrayList<>();
        options.add(new Option(KafkaOptions.START, ExpressionTestUtils.createStringExpression("earliest")));
        options.add(new Option(KafkaOptions.SORT_ORDER, ExpressionTestUtils.createStringExpression("newest")));

        IDatasource ds = catalog.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("topic", TEST_TOPIC),
                new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, options));

        List<RowOrder> rows = new ArrayList<>();
        TupleIterator it = ds.execute(context);
        try
        {
            while (it.hasNext())
            {
                TupleVector batch = it.next();
                for (int i = 0; i < batch.getRowCount(); i++)
                {
                    rows.add(new RowOrder(getTimestampEpoch(batch, i), (long) batch.getColumn(KafkaTupleIterator.COL_OFFSET)
                            .getAny(i),
                            (int) batch.getColumn(KafkaTupleIterator.COL_PARTITION)
                                    .getAny(i)));
                }
            }
        }
        finally
        {
            it.close();
        }

        assertEquals(1, sortItems.size(), "Non-pushable sort item should remain for engine sorting");
        assertEquals(NUM_MESSAGES, rows.size());
        assertNewestOrder(rows);
    }

    private static void assertNewestOrder(List<RowOrder> rows)
    {
        for (int i = 1; i < rows.size(); i++)
        {
            RowOrder prev = rows.get(i - 1);
            RowOrder current = rows.get(i);

            assertTrue(prev.timestamp >= current.timestamp, "Rows should be sorted by timestamp DESC");

            if (prev.timestamp == current.timestamp)
            {
                assertTrue(prev.offset > current.offset
                        || prev.offset == current.offset
                                && prev.partition <= current.partition,
                        "Rows should be sorted by offset DESC and partition ASC for ties");
            }
        }
    }

    private static long getTimestampEpoch(TupleVector batch, int row)
    {
        return ((se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset) batch.getColumn(KafkaTupleIterator.COL_TIMESTAMP)
                .getAny(row)).getEpoch();
    }

    private record RowOrder(long timestamp, long offset, int partition)
    {
    }
}
