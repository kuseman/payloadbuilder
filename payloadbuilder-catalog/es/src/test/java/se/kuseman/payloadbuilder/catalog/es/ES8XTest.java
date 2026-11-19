package se.kuseman.payloadbuilder.catalog.es;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;

/** Test with elastic search 8.X */
class ES8XTest extends BaseESTest
{
    ES8XTest()
    {
        super(ES.ES_ENDPOINT, ESCatalog.SINGLE_TYPE_TABLE_NAME, Version._8X);
    }

    @AfterAll
    static void tearDownClass()
    {
        ES.stop();
    }

    @Override
    protected Map<String, Object> modifyMappingProperties(Map<String, Object> mappings)
    {
        // ES 8.X doesn't have any types so return mappings plainly
        return mappings;
    }

    @Override
    protected ValueVector getTypeColumnExpected(int size)
    {
        // ES 8.X doesn't have any type fields
        return ValueVector.literalNull(ResolvedType.of(Type.Any), size);
    }

    static class ES
    {
        private static final int PORT = 9200;
        private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:8.7.1";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> ES_CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT)
        //@formatter:off
                .withEnv(Map.of(
                        "xpack.security.enabled", "false",
                        "discovery.type", "single-node",
                        "cluster.routing.allocation.disk.watermark.high", "99%",
                        "cluster.routing.allocation.disk.watermark.flood_stage", "99%",
                        "cluster.routing.allocation.disk.watermark.high.max_headroom", "20GB",
                        "cluster.routing.allocation.disk.watermark.low.max_headroom", "20GB",
                        "cluster.routing.allocation.disk.watermark.flood_stage.max_headroom", "20GB"
                        ));
        //@formatter:on

        static final String ES_ENDPOINT;

        static
        {
            ES_CONTAINER.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*\"license.*valid\".*"));
            ES_CONTAINER.withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
            ES_CONTAINER.start();

            ES_ENDPOINT = "http://" + ES_CONTAINER.getHost() + ":" + ES_CONTAINER.getMappedPort(PORT);

            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> ES_CONTAINER.stop()));
        }

        static void stop()
        {
            if (ES_CONTAINER != null)
            {
                ES_CONTAINER.stop();
            }
        }
    }
}
